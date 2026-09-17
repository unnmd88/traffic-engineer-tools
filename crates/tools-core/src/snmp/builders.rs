use crate::snmp::{
    ParseError,
    oid::SnmpOid,
    parsers::bit_mask_ug405::parse_utc_bitmask,
    value::{SnmpValue, SnmpValueType},
};

pub type OidValueBuilderFn = fn(&str) -> Result<SnmpValue, ParseError>;

const SWARCO_MAX_STAGE: u32 = 8;

fn parse_to_u32(value: &str) -> Result<u32, ParseError> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| ParseError::InvalidType {
            expected: "Number (Unsigned32)".to_string(),
            actual: value.to_string(),
        })
}

pub fn to_stage_val_stcip(value: &str) -> Result<SnmpValue, ParseError> {
    parse_to_u32(value).map(SnmpValue::Gauge32)
}

pub fn to_stage_val_swarco_8stages(value: &str) -> Result<SnmpValue, ParseError> {
    let val = parse_to_u32(value)?;

    if val > SWARCO_MAX_STAGE {
        return Err(ParseError::InvalidValue {
            value: value.to_string(),
            reason: "stage > 8 not allowed for Swarco ITC-2".to_string(),
        });
    }

    // Swarco ITC-2 кодирует фазу 8 как 1.
    Ok(SnmpValue::Gauge32(if val == 8 { 1 } else { val }))
}

fn stage_to_bitmask(stage: u32) -> Result<Vec<u8>, ParseError> {
    if stage == 0 || stage > 64 {
        return Err(ParseError::InvalidValue {
            value: stage.to_string(),
            reason: "stage must be in range 1..64".to_string(),
        });
    }

    // Позиция бита. индексируется с 0, поэтому - 1.
    let bit = stage - 1;

    // биты 0..7 → 1 байт,
    // биты 8..15 → 2 байта
    // биты 16..23 → 3 байта
    // ...
    let nbytes = (bit / 8 + 1) as usize;
    let byte_from_lsb = (bit / 8) as usize;
    let bit_in_byte = (bit % 8) as usize;

    let mut buf = vec![0u8; nbytes];
    // big-endian: младший байт — последний
    buf[nbytes - 1 - byte_from_lsb] = 1u8 << bit_in_byte;
    Ok(buf)
}

pub fn to_stage_u405(value: &str) -> Result<SnmpValue, ParseError> {
    // Ожидается либо hex-строка, если есть префикс 0x, либо число
    // Например: 0x 02 00, 4, 12, 0x02
    let sanitized_value = value.trim().to_lowercase().replace(' ', "");

    if sanitized_value.is_empty() {
        return Err(ParseError::CantBeEmpty {
            name: "Value".to_string(),
        });
    }

    let bytes = match sanitized_value.strip_prefix("0x") {
        Some(hex) => {
            let bytes = hex::decode(hex).map_err(|_| ParseError::InvalidValue {
                value: value.to_string(),
                reason: "Invalid hex-string".to_string(),
            })?;
            parse_utc_bitmask(&bytes)?;
            bytes
        }
        None => {
            if sanitized_value.len() > 1 && sanitized_value.starts_with("0") {
                return Err(ParseError::InvalidValue {
                    value: value.to_string(),
                    reason: "Invalid digit: can`t starts whith 0".to_string(),
                });
            }

            let as_digit =
                sanitized_value
                    .parse::<u32>()
                    .map_err(|_| ParseError::InvalidValue {
                        value: value.to_string(),
                        reason: "Allowed numbers in range 1..64".to_string(),
                    })?;
            stage_to_bitmask(as_digit)?
        }
    };

    Ok(SnmpValue::OctetString(bytes))
}

pub fn to_i32(value: &str) -> Result<SnmpValue, ParseError> {
    encode_by_type(SnmpValueType::Integer, value)
}

pub fn to_u32(value: &str) -> Result<SnmpValue, ParseError> {
    encode_by_type(SnmpValueType::Gauge32, value)
}

/// Кастомный OID (нет в реестре): кодируем значение по явному типу.
/// `OctetString` ждёт hex (с `0x` или без), числа — десятичные, `IpAddress` — `a.b.c.d`.
pub fn encode_by_type(ty: SnmpValueType, raw: &str) -> Result<SnmpValue, ParseError> {
    let raw = raw.trim();
    match ty {
        SnmpValueType::OctetString => {
            let hex = raw.strip_prefix("0x").unwrap_or(raw).replace(' ', "");
            let bytes = hex::decode(&hex).map_err(|_| ParseError::InvalidValue {
                value: raw.to_string(),
                reason: "invalid hex octet string".to_string(),
            })?;
            Ok(SnmpValue::OctetString(bytes))
        }
        SnmpValueType::Integer => {
            raw.parse::<i32>()
                .map(SnmpValue::Integer)
                .map_err(|_| ParseError::InvalidValue {
                    value: raw.to_string(),
                    reason: "invalid i32 for Integer".to_string(),
                })
        }
        SnmpValueType::Gauge32 | SnmpValueType::Unsigned32 => raw
            .parse::<u32>()
            .map(SnmpValue::Gauge32)
            .map_err(|_| ParseError::InvalidValue {
                value: raw.to_string(),
                reason: "invalid u32 for Gauge32/Unsigned32".to_string(),
            }),

        SnmpValueType::Counter32 => {
            raw.parse::<u32>()
                .map(SnmpValue::Counter32)
                .map_err(|_| ParseError::InvalidValue {
                    value: raw.to_string(),
                    reason: "invalid u32 for Counter32".to_string(),
                })
        }
        SnmpValueType::Counter64 => {
            raw.parse::<u64>()
                .map(SnmpValue::Counter64)
                .map_err(|_| ParseError::InvalidValue {
                    value: raw.to_string(),
                    reason: "invalid u64 for Counter64".to_string(),
                })
        }
        SnmpValueType::TimeTicks => {
            raw.parse::<u32>()
                .map(SnmpValue::TimeTicks)
                .map_err(|_| ParseError::InvalidValue {
                    value: raw.to_string(),
                    reason: "invalid u32 for TimeTicks".to_string(),
                })
        }
        SnmpValueType::IpAddress => {
            let parts: Vec<u8> = raw
                .split('.')
                .map(|p| p.parse::<u8>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ParseError::InvalidValue {
                    value: raw.to_string(),
                    reason: "invalid IPv4 address".to_string(),
                })?;
            if parts.len() != 4 {
                return Err(ParseError::InvalidValue {
                    value: raw.to_string(),
                    reason: "IPv4 must be a.b.c.d".to_string(),
                });
            }
            Ok(SnmpValue::IpAddress([parts[0], parts[1], parts[2], parts[3]]))
        }
        SnmpValueType::Oid => {
            let oid = SnmpOid::parse(raw).map_err(|_| ParseError::InvalidValue {
                value: raw.to_string(),
                reason: "invalid OID".to_string(),
            })?;
            Ok(SnmpValue::Oid(oid))
        }
        other => Err(ParseError::InvalidValue {
            value: raw.to_string(),
            reason: format!("type {other} is not supported for SET"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snmp::parsers::bit_mask_ug405::parse_utc_bitmask;

    fn octet(value: SnmpValue) -> Vec<u8> {
        match value {
            SnmpValue::OctetString(bytes) => bytes,
            other => panic!("expected OctetString, got {other:?}"),
        }
    }

    fn gauge(value: SnmpValue) -> u32 {
        match value {
            SnmpValue::Gauge32(n) => n,
            other => panic!("expected Gauge32, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_stage_to_bitmask_and_back() {
        for stage in 1..=64u32 {
            let bytes = stage_to_bitmask(stage).unwrap();
            assert_eq!(parse_utc_bitmask(&bytes).unwrap(), stage, "stage {stage}");
        }
    }

    #[test]
    fn to_stage_u405_from_decimal() {
        assert_eq!(octet(to_stage_u405("1").unwrap()), vec![0x01]);
        assert_eq!(octet(to_stage_u405("8").unwrap()), vec![0x80]);
        assert_eq!(octet(to_stage_u405("9").unwrap()), vec![0x01, 0x00]);
        assert_eq!(octet(to_stage_u405("16").unwrap()), vec![0x80, 0x00]);
        assert_eq!(
            octet(to_stage_u405("64").unwrap()),
            vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn to_stage_u405_from_hex() {
        assert_eq!(octet(to_stage_u405("0x01").unwrap()), vec![0x01]);
        assert_eq!(octet(to_stage_u405("0x80").unwrap()), vec![0x80]);
        assert_eq!(octet(to_stage_u405("0x0100").unwrap()), vec![0x01, 0x00]);
        // пробелы игнорируются
        assert_eq!(octet(to_stage_u405("0x 01 00").unwrap()), vec![0x01, 0x00]);
    }

    #[test]
    fn to_stage_u405_rejects_invalid() {
        assert!(to_stage_u405("0x0300").is_err()); // два активных бита
        assert!(to_stage_u405("0x").is_err()); // пустой hex
        assert!(to_stage_u405("0").is_err()); // стадия 0
        assert!(to_stage_u405("65").is_err()); // вне 1..64
        assert!(to_stage_u405("01").is_err()); // ведущий ноль
        assert!(to_stage_u405("").is_err()); // пустая строка
        assert!(to_stage_u405("abc").is_err()); // не число
    }

    #[test]
    fn to_stage_val_swarco_8stages_mapping() {
        assert_eq!(gauge(to_stage_val_swarco_8stages("3").unwrap()), 3);
        // Swarco ITC-2 кодирует стадию 8 как 1
        assert_eq!(gauge(to_stage_val_swarco_8stages("8").unwrap()), 1);
        assert!(to_stage_val_swarco_8stages("9").is_err()); // > 8
    }

    #[test]
    fn to_stage_val_stcip_passthrough() {
        assert_eq!(gauge(to_stage_val_stcip("3").unwrap()), 3);
        assert_eq!(gauge(to_stage_val_stcip("42").unwrap()), 42);
        assert!(to_stage_val_stcip("abc").is_err());
    }

    #[test]
    fn encode_by_type_octet_string() {
        assert_eq!(octet(encode_by_type(SnmpValueType::OctetString, "0x01").unwrap()), vec![0x01]);
        assert_eq!(
            octet(encode_by_type(SnmpValueType::OctetString, "01 02").unwrap()),
            vec![0x01, 0x02]
        );
        assert!(encode_by_type(SnmpValueType::OctetString, "zz").is_err());
    }

    #[test]
    fn encode_by_type_numbers() {
        assert_eq!(gauge(encode_by_type(SnmpValueType::Gauge32, "42").unwrap()), 42);
        assert!(matches!(
            encode_by_type(SnmpValueType::Integer, "-5").unwrap(),
            SnmpValue::Integer(-5)
        ));
        assert!(matches!(
            encode_by_type(SnmpValueType::Counter64, "99").unwrap(),
            SnmpValue::Counter64(99)
        ));
    }

    #[test]
    fn encode_by_type_ip_and_oid() {
        assert!(matches!(
            encode_by_type(SnmpValueType::IpAddress, "127.0.0.1").unwrap(),
            SnmpValue::IpAddress([127, 0, 0, 1])
        ));
        assert!(encode_by_type(SnmpValueType::IpAddress, "127.0.0").is_err());
        assert!(encode_by_type(SnmpValueType::Oid, "1.3.6.1").is_ok());
        assert!(encode_by_type(SnmpValueType::Oid, "not-an-oid").is_err());
    }

    #[test]
    fn encode_by_type_rejects_unsupported() {
        assert!(encode_by_type(SnmpValueType::Null, "x").is_err());
        assert!(encode_by_type(SnmpValueType::NoSuchObject, "x").is_err());
        assert!(encode_by_type(SnmpValueType::Opaque, "x").is_err());
    }
}
