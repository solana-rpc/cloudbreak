pub const REWARD_NUM: usize = 3;
pub const TICK_ARRAY_SIZE: usize = 60;

pub const REWARD_INFO_SPAN: usize = 1 + 8 * 3 + 16 + 8 * 2 + 32 * 3 + 16; 

pub const DYNAMIC_FEE_INFO_SPAN: usize = 2 * 3 + 4 * 5 + 8 + 46; 

pub const TICK_SPAN: usize = 4 + 16 + 16 + 16 + 16 + 16 * REWARD_NUM + 8 * 3 + 16 + 4 * 3; 

pub const POOL_INFO_SPAN: usize = 1544;
pub const PERSONAL_POSITION_SPAN: usize = 281;
pub const TICK_ARRAY_SPAN: usize = 10240;

#[derive(Debug, Clone, Copy)]
pub struct PoolInfo {
    pub liquidity: u128,
    pub tick_current: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct PersonalPosition {
    pub pool_id: [u8; 32],
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity: u128,
}

#[derive(Debug, Clone, Copy)]
pub struct Tick {
    pub tick: i32,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
}

#[derive(Debug, Clone)]
pub struct TickArray {
    pub pool_id: [u8; 32],
    pub ticks: Vec<Tick>,
}

pub fn decode_pool_info(data: &[u8]) -> PoolInfo {
    let mut r = Reader::new(data);

    r.skip(8);
    r.skip(1);
    r.skip(32);
    r.skip(32);
    r.skip(32);
    r.skip(32);
    r.skip(32);
    r.skip(32);
    r.skip(32);
    r.skip(1);
    r.skip(1);
    r.skip(2);
    let liquidity = r.u128();
    r.skip(16);
    let tick_current = r.i32();
    r.skip(2);
    r.skip(2);
    r.skip(16);
    r.skip(16);
    r.skip(8);
    r.skip(8);
    r.skip(16 * 4);
    r.skip(1);
    r.skip(1);
    r.skip(6);
    r.skip(REWARD_INFO_SPAN * REWARD_NUM); 
    r.skip(8 * 16);
    r.skip(8 * 4);
    r.skip(8);
    r.skip(8);
    r.skip(8);
    r.skip(8);
    r.skip(DYNAMIC_FEE_INFO_SPAN);
    r.skip(8 * 46);

    assert_eq!(r.pos(), POOL_INFO_SPAN, "PoolInfoLayout span mismatch");

    PoolInfo {
        liquidity,
        tick_current,
    }
}

pub fn decode_personal_position(data: &[u8]) -> PersonalPosition {
    let mut r = Reader::new(data);

    r.skip(8);
    r.skip(1);
    r.skip(32);
    let pool_id = r.pubkey();
    let tick_lower = r.i32();
    let tick_upper = r.i32();
    let liquidity = r.u128();
    r.skip(16);
    r.skip(16);
    r.skip(8);
    r.skip(8);
    r.skip((16 + 8) * REWARD_NUM);
    r.skip(8);
    r.skip(8 * 7);

    assert_eq!(
        r.pos(),
        PERSONAL_POSITION_SPAN,
        "PersonalPositionLayout span mismatch"
    );

    PersonalPosition {
        pool_id,
        tick_lower,
        tick_upper,
        liquidity,
    }
}

pub fn decode_tick_array(data: &[u8]) -> TickArray {
    let mut r = Reader::new(data);

    r.skip(8);
    let pool_id = r.pubkey();
    r.skip(4);

    
    let mut ticks = Vec::with_capacity(TICK_ARRAY_SIZE);
    for _ in 0..TICK_ARRAY_SIZE {
        let start = r.pos();

        let tick = r.i32();
        let liquidity_net = r.i128();
        let liquidity_gross = r.u128(); 
        r.skip(16);
        r.skip(16);
        r.skip(16 * REWARD_NUM);
        r.skip(8);
        r.skip(8);
        r.skip(8);
        r.skip(16);
        r.skip(4 * 3);

        assert_eq!(r.pos() - start, TICK_SPAN, "TickLayout span mismatch");

        ticks.push(Tick {
            tick,
            liquidity_net,
            liquidity_gross,
        });
    }

    r.skip(1);
    r.skip(8);
    r.skip(107);

    assert_eq!(r.pos(), TICK_ARRAY_SPAN, "TickArrayLayout span mismatch");

    TickArray { pool_id, ticks }
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    fn pos(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> &'a [u8] {
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        s
    }

    fn skip(&mut self, n: usize) {
        self.pos += n;
    }

    fn i32(&mut self) -> i32 {
        i32::from_le_bytes(self.take(4).try_into().unwrap())
    }

    fn u128(&mut self) -> u128 {
        u128::from_le_bytes(self.take(16).try_into().unwrap())
    }
    
    fn i128(&mut self) -> i128 {
        i128::from_le_bytes(self.take(16).try_into().unwrap())
    }

    fn pubkey(&mut self) -> [u8; 32] {
        self.take(32).try_into().unwrap()
    }
}
