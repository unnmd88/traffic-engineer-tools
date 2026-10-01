use crate::snmp::{ParseError, business_value::BusinessValue, value::SnmpValue};

/*
Соответствие цветов байтам:
OFF = 0
RED_AMBER = 1
RED = 2
GREEN = 3
GREEN_FLASH = 4
AMBER = 5
AMBER_FLASH = 6
ARROW_RED_AMBER = 7
ARROW_RED = 8
ARROW_GREEN = 9
ARROW_GREEN_FLASH = 10
ARROW_AMBER = 11
ARROW_FLASH_RED_AMBER = 13
ARROW_FLASH_RED = 14
ARROW_FLASH_GREEN = 15
ARROW_FLASH_GREEN_FLASH = 16
ARROW_FLASH_AMBER = 17
ARROW = 21
ARROW_FLASH = 22
*/

#[derive(Debug, Clone)]
pub struct GroupStatePotokUg405 {
    num_group: u8,
    source_code: u8,
    state: Option<GroupState>,
}

impl GroupStatePotokUg405 {
    pub fn from_byte(num_group: u8, byte_code: u8) -> Self {
        Self {
            num_group,
            source_code: byte_code,
            state: GroupState::from_byte(byte_code),
        }
    }

    pub fn source_code(&self) -> u8 {
        self.source_code
    }

    pub fn num_group(&self) -> u8 {
        self.num_group
    }

    pub fn state(&self) -> Option<&GroupState> {
        self.state.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Off,
    RedAmber,
    Red,
    Green,
    GreenFlash,
    Amber,
    AmberFlash,
    ArrowRedAmber,
    ArrowRed,
    ArrowGreen,
    ArrowGreenFlash,
    ArrowAmber,
    ArrowFlashRedAmber,
    ArrowFlashRed,
    ArrowFlashGreen,
    ArrowFlashGreenFlash,
    ArrowFlashAmber,
    Arrow,
    ArrowFlash,
}

impl GroupState {
    pub const OFF: &'static str = "OFF";
    pub const RED_AMBER: &'static str = "RED_AMBER";
    pub const RED: &'static str = "RED";
    pub const GREEN: &'static str = "GREEN";
    pub const GREEN_FLASH: &'static str = "GREEN_FLASH";
    pub const AMBER: &'static str = "AMBER";
    pub const AMBER_FLASH: &'static str = "AMBER_FLASH";
    pub const ARROW_RED_AMBER: &'static str = "ARROW_RED_AMBER";
    pub const ARROW_RED: &'static str = "ARROW_RED";
    pub const ARROW_GREEN: &'static str = "ARROW_GREEN";
    pub const ARROW_GREEN_FLASH: &'static str = "ARROW_GREEN_FLASH";
    pub const ARROW_AMBER: &'static str = "ARROW_AMBER";
    pub const ARROW_FLASH_RED_AMBER: &'static str = "ARROW_FLASH_RED_AMBER";
    pub const ARROW_FLASH_RED: &'static str = "ARROW_FLASH_RED";
    pub const ARROW_FLASH_GREEN: &'static str = "ARROW_FLASH_GREEN";
    pub const ARROW_FLASH_GREEN_FLASH: &'static str = "ARROW_FLASH_GREEN_FLASH";
    pub const ARROW_FLASH_AMBER: &'static str = "ARROW_FLASH_AMBER";
    pub const ARROW: &'static str = "ARROW";
    pub const ARROW_FLASH: &'static str = "ARROW_FLASH";

    pub fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0 => Self::Off,
            1 => Self::RedAmber,
            2 => Self::Red,
            3 => Self::Green,
            4 => Self::GreenFlash,
            5 => Self::Amber,
            6 => Self::AmberFlash,
            7 => Self::ArrowRedAmber,
            8 => Self::ArrowRed,
            9 => Self::ArrowGreen,
            10 => Self::ArrowGreenFlash,
            11 => Self::ArrowAmber,
            13 => Self::ArrowFlashRedAmber,
            14 => Self::ArrowFlashRed,
            15 => Self::ArrowFlashGreen,
            16 => Self::ArrowFlashGreenFlash,
            17 => Self::ArrowFlashAmber,
            21 => Self::Arrow,
            22 => Self::ArrowFlash,
            _ => return None,
        })
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::RedAmber => 1,
            Self::Red => 2,
            Self::Green => 3,
            Self::GreenFlash => 4,
            Self::Amber => 5,
            Self::AmberFlash => 6,
            Self::ArrowRedAmber => 7,
            Self::ArrowRed => 8,
            Self::ArrowGreen => 9,
            Self::ArrowGreenFlash => 10,
            Self::ArrowAmber => 11,
            Self::ArrowFlashRedAmber => 13,
            Self::ArrowFlashRed => 14,
            Self::ArrowFlashGreen => 15,
            Self::ArrowFlashGreenFlash => 16,
            Self::ArrowFlashAmber => 17,
            Self::Arrow => 21,
            Self::ArrowFlash => 22,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => Self::OFF,
            Self::RedAmber => Self::RED_AMBER,
            Self::Red => Self::RED,
            Self::Green => Self::GREEN,
            Self::GreenFlash => Self::GREEN_FLASH,
            Self::Amber => Self::AMBER,
            Self::AmberFlash => Self::AMBER_FLASH,
            Self::ArrowRedAmber => Self::ARROW_RED_AMBER,
            Self::ArrowRed => Self::ARROW_RED,
            Self::ArrowGreen => Self::ARROW_GREEN,
            Self::ArrowGreenFlash => Self::ARROW_GREEN_FLASH,
            Self::ArrowAmber => Self::ARROW_AMBER,
            Self::ArrowFlashRedAmber => Self::ARROW_FLASH_RED_AMBER,
            Self::ArrowFlashRed => Self::ARROW_FLASH_RED,
            Self::ArrowFlashGreen => Self::ARROW_FLASH_GREEN,
            Self::ArrowFlashGreenFlash => Self::ARROW_FLASH_GREEN_FLASH,
            Self::ArrowFlashAmber => Self::ARROW_FLASH_AMBER,
            Self::Arrow => Self::ARROW,
            Self::ArrowFlash => Self::ARROW_FLASH,
        }
    }

    pub fn split_state(&self) -> (&'static str, &'static str) {
        match self {
            Self::Off => (Self::OFF, Self::OFF),
            Self::RedAmber => (Self::RED_AMBER, Self::OFF),
            Self::Red => (Self::RED, Self::OFF),
            Self::Green => (Self::GREEN, Self::OFF),
            Self::GreenFlash => (Self::GREEN_FLASH, Self::OFF),
            Self::Amber => (Self::AMBER, Self::OFF),
            Self::AmberFlash => (Self::AMBER_FLASH, Self::OFF),
            Self::ArrowRedAmber => (Self::RED_AMBER, Self::GREEN),
            Self::ArrowRed => (Self::RED, Self::GREEN),
            Self::ArrowGreen => (Self::GREEN, Self::GREEN),
            Self::ArrowGreenFlash => (Self::GREEN_FLASH, Self::GREEN),
            Self::ArrowAmber => (Self::AMBER, Self::GREEN),
            Self::ArrowFlashRedAmber => (Self::RED_AMBER, Self::GREEN_FLASH),
            Self::ArrowFlashRed => (Self::RED, Self::GREEN_FLASH),
            Self::ArrowFlashGreen => (Self::GREEN, Self::GREEN_FLASH),
            Self::ArrowFlashGreenFlash => (Self::GREEN_FLASH, Self::GREEN_FLASH),
            Self::ArrowFlashAmber => (Self::AMBER, Self::GREEN_FLASH),
            Self::Arrow => (Self::OFF, Self::GREEN),
            Self::ArrowFlash => (Self::GREEN_FLASH, Self::GREEN),
        }
    }
}

impl std::fmt::Display for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
