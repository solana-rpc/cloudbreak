// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! TCP stream wrapper that keeps the kernel receive time (SO_TIMESTAMPNS) of the last bytes read.
//! `pre_handler_us` on the `http_request` span is the handler start minus that time.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Kernel receive time of the last packet read on a connection, in Unix nanoseconds (0 = none yet).
#[derive(Clone, Default)]
pub struct LastRx(Arc<AtomicU64>);

impl LastRx {
    /// Microseconds from the last received packet to now, or `None` before the first timestamped read.
    pub fn elapsed_us(&self) -> Option<u64> {
        let last_ns = self.0.load(Ordering::Relaxed);
        if last_ns == 0 {
            return None;
        }
        let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_nanos() as u64;
        Some(now_ns.saturating_sub(last_ns) / 1_000)
    }
}

pub struct TimestampedStream {
    inner: TcpStream,
    last_rx: LastRx,
}

impl TimestampedStream {
    /// Wraps an accepted stream. Without SO_TIMESTAMPNS (non-Linux or setsockopt failure) reads work as usual.
    pub fn new(inner: TcpStream) -> (Self, LastRx) {
        #[cfg(target_os = "linux")]
        linux::enable_rx_timestamps(&inner);
        let last_rx = LastRx::default();
        (
            Self {
                inner,
                last_rx: last_rx.clone(),
            },
            last_rx,
        )
    }
}

impl AsyncRead for TimestampedStream {
    #[cfg(target_os = "linux")]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            std::task::ready!(this.inner.poll_read_ready(cx))?;
            let unfilled = buf.initialize_unfilled();
            match this.inner.try_io(tokio::io::Interest::READABLE, || {
                linux::recv_with_timestamp(&this.inner, unfilled)
            }) {
                Ok((n, rx_ns)) => {
                    if let Some(rx_ns) = rx_ns {
                        this.last_rx.0.store(rx_ns, Ordering::Relaxed);
                    }
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TimestampedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::os::fd::AsRawFd;

    use tokio::net::TcpStream;

    pub fn enable_rx_timestamps(stream: &TcpStream) {
        let on: libc::c_int = 1;
        // SAFETY: valid socket fd and a correctly sized option value.
        let rc = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPNS,
                (&on as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            tracing::debug!("SO_TIMESTAMPNS not enabled: {}", io::Error::last_os_error());
        }
    }

    /// `recvmsg` into `buf`; returns the bytes read and the SCM_TIMESTAMPNS receive time in Unix ns.
    pub fn recv_with_timestamp(
        stream: &TcpStream,
        buf: &mut [u8],
    ) -> io::Result<(usize, Option<u64>)> {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        // u64 array keeps the control buffer aligned for cmsghdr; 64 bytes fit one timespec message.
        let mut control = [0u64; 8];
        // SAFETY: msghdr is plain data; the pointers below stay valid for the recvmsg call.
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = std::mem::size_of_val(&control) as _;

        // SAFETY: fd is a valid socket; msg points to live buffers of the declared sizes.
        let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut rx_ns = None;
        // SAFETY: CMSG_* walk the control buffer that recvmsg filled, within msg_controllen.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET
                    && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                {
                    let ts: libc::timespec =
                        std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast());
                    rx_ns = Some(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64);
                }
                cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
            }
        }
        Ok((n as usize, rx_ns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn records_kernel_receive_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (mut server, last_rx) = TimestampedStream::new(server);
        assert_eq!(last_rx.elapsed_us(), None);

        client.write_all(b"ping").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        // The bytes arrived ~50 ms before they were read, so the elapsed time includes that wait.
        let elapsed = last_rx.elapsed_us().expect("receive timestamp recorded");
        assert!(elapsed >= 45_000, "elapsed {elapsed} us");
        assert!(elapsed < 5_000_000, "elapsed {elapsed} us");
    }
}
