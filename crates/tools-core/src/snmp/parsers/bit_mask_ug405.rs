use crate::snmp::ParseError;

/// Парсит OCTET STRING в соответствии со спецификацией UTCType2BitMask.
///
/// Байты идут от младшего к старшему (LSB first).
/// Возвращает номер активного бита (1-based).
///
/// # Ошибки
/// - Пустой массив
/// - Байт не является степенью двойки
/// - Несколько байтов с активными битами
pub fn parse_utc_bitmask(bytes: &[u8]) -> Result<u32, ParseError> {
    if bytes.is_empty() {
        tracing::warn!(target: "parse_utc_bitmask", "bytes is empty");
        return Err(ParseError::CantBeEmpty {
            name: "UTCType2BitMask".to_string(),
        });
    }

    let mut found_pos: Option<usize> = None;
    let mut stage = 0;

    // Байты идут от старшего к младшему (MSB first): младший бит (LSB) всей
    // маски — в последнем байте. Номер бита считаем от младшего.
    for (idx_from_end, &byte) in bytes.iter().rev().enumerate() {
        if byte == 0 {
            continue;
        }

        let pos = bytes.len() - idx_from_end - 1;

        // Байт должен быть степенью двойки (ровно один активный бит).
        if byte & (byte - 1) != 0 {
            let err_msg = format!(
                "Byte must be power of 2. Got: {} (0b{:08b}) at position {}",
                byte, byte, pos
            );

            tracing::error!(target: "Parse UtcReplyGn", bytes = ?bytes, "{}", &err_msg);

            return Err(ParseError::Common { message: err_msg });
        }

        if let Some(first) = found_pos {
            let err_msg = format!(
                "Multiple bytes with active bits. First at position {}, second at position {}",
                first, pos
            );

            tracing::error!(target: "parse_utc_bitmask", bytes = ?bytes, "{}", &err_msg);

            return Err(ParseError::Common { message: err_msg });
        }

        found_pos = Some(pos);
        stage = (idx_from_end as u32 * 8) + byte.trailing_zeros() + 1;
    }

    if found_pos.is_none() {
        let err_msg = "No active bits found (all bytes are zero)".to_string();
        tracing::error!(target: "parse_utc_bitmask", bytes = ?bytes, "{}", &err_msg);
        return Err(ParseError::Common { message: err_msg });
    }

    Ok(stage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_utc_bitmask_stage_1() {
        let bytes = vec![0x00, 0x00, 0x00, 0x01];
        let result = parse_utc_bitmask(&bytes).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_parse_utc_bitmask_stage_9() {
        let bytes = vec![0x00, 0x00, 0x01, 0x00];
        let result = parse_utc_bitmask(&bytes).unwrap();
        assert_eq!(result, 9);
    }

    #[test]
    fn test_parse_utc_bitmask_stage_17() {
        let bytes = vec![0x00, 0x01, 0x00, 0x00];
        let result = parse_utc_bitmask(&bytes).unwrap();
        assert_eq!(result, 17);
    }

    #[test]
    fn test_parse_utc_bitmask_error_not_power_of_two() {
        let bytes = vec![0x03, 0x00, 0x00, 0x00]; // 3 = 0b11
        let result = parse_utc_bitmask(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_utc_bitmask_error_empty() {
        let bytes: Vec<u8> = vec![];
        let result = parse_utc_bitmask(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_stage() {
        assert_eq!(parse_utc_bitmask(&[1]).unwrap(), 1);
        assert_eq!(parse_utc_bitmask(&[2]).unwrap(), 2);
        assert_eq!(parse_utc_bitmask(&[4]).unwrap(), 3);
        assert_eq!(parse_utc_bitmask(&[8]).unwrap(), 4);
        assert_eq!(parse_utc_bitmask(&[16]).unwrap(), 5);
        assert_eq!(parse_utc_bitmask(&[32]).unwrap(), 6);
        assert_eq!(parse_utc_bitmask(&[64]).unwrap(), 7);
        assert_eq!(parse_utc_bitmask(&[128]).unwrap(), 8);
        assert_eq!(parse_utc_bitmask(&[1, 0]).unwrap(), 9);
        assert_eq!(parse_utc_bitmask(&[2, 0]).unwrap(), 10);
        assert_eq!(parse_utc_bitmask(&[0, 1]).unwrap(), 1);
    }

    #[test]
    fn test_parse_stage_errors() {
        assert!(parse_utc_bitmask(&[]).is_err());
        assert!(parse_utc_bitmask(&[3]).is_err()); // 0b11
        assert!(parse_utc_bitmask(&[5]).is_err()); // 0b101
        assert!(parse_utc_bitmask(&[1, 1]).is_err());
        assert!(parse_utc_bitmask(&[0]).is_err());
    }
}
