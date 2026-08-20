use crate::error::ParseError;

pub fn parse_gn_utc_reply_to_stage(octet_string_as_bytes: &[u8]) -> Result<u32, ParseError> {
    // Проверка на пустую строку
    if octet_string_as_bytes.is_empty() {
        tracing::warn!(target: "Parse UtcReplyGn", "OctetString is empty");
        return Err(ParseError::CantBeEmpty {
            name: "OctetString UtcReplyGn".to_string(),
        });
    }

    // Предупреждение о большом размере (но не ошибка)
    if octet_string_as_bytes.len() > 4 {
        tracing::warn!(
            target: "Parse UtcReplyGn",
            len = octet_string_as_bytes.len(),
            bytes = ?octet_string_as_bytes,
            "Stage value may exceed 256 (more than 4 bytes)"
        );
    }

    let mut found_bit = false;
    let mut stage = 0;

    // Идем с конца (младший байт первый)
    for (idx_from_end, &byte) in octet_string_as_bytes.iter().rev().enumerate() {
        if byte == 0 {
            continue;
        }

        // Проверка: байт должен быть степенью двойки
        if byte & (byte - 1) != 0 {
            let err_msg = format!(
                "Byte must be power of 2. Got: {} (0b{:08b}) at position {}",
                byte,
                byte,
                octet_string_as_bytes.len() - idx_from_end - 1
            );

            tracing::error!(
                target: "Parse UtcReplyGn",
                bytes = ?octet_string_as_bytes,
                "{}", &err_msg,
            );

            return Err(ParseError::Common { message: err_msg });
        }

        // Проверка: только один байт с активным битом
        if found_bit {
            let err_msg = format!(
                "Multiple bytes with active bits. First at position {}, second at position {}",
                octet_string_as_bytes.len() - idx_from_end - 1,
                octet_string_as_bytes.len() - idx_from_end - 1
            );

            tracing::error!(
                target: "Parse UtcReplyGn",
                bytes = ?octet_string_as_bytes,
                "{}", &err_msg,
            );

            return Err(ParseError::Common { message: err_msg });
        }

        found_bit = true;
        stage = (idx_from_end as u32 * 8) + byte.trailing_zeros() + 1;
    }

    if !found_bit {
        let err_msg = "No active bits found (all bytes are zero)".to_string();
        tracing::error!(
            target: "Parse UtcReplyGn",
            bytes = ?octet_string_as_bytes,
            "{}", &err_msg,
        );
        return Err(ParseError::Common { message: err_msg });
    }

    Ok(stage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stage() {
        assert_eq!(parse_gn_utc_reply_to_stage(&[1]).unwrap(), 1);
        assert_eq!(parse_gn_utc_reply_to_stage(&[2]).unwrap(), 2);
        assert_eq!(parse_gn_utc_reply_to_stage(&[4]).unwrap(), 3);
        assert_eq!(parse_gn_utc_reply_to_stage(&[8]).unwrap(), 4);
        assert_eq!(parse_gn_utc_reply_to_stage(&[16]).unwrap(), 5);
        assert_eq!(parse_gn_utc_reply_to_stage(&[32]).unwrap(), 6);
        assert_eq!(parse_gn_utc_reply_to_stage(&[64]).unwrap(), 7);
        assert_eq!(parse_gn_utc_reply_to_stage(&[128]).unwrap(), 8);
        assert_eq!(parse_gn_utc_reply_to_stage(&[1, 0]).unwrap(), 9);
        assert_eq!(parse_gn_utc_reply_to_stage(&[2, 0]).unwrap(), 10);
        assert_eq!(parse_gn_utc_reply_to_stage(&[0, 1]).unwrap(), 1);
    }

    #[test]
    fn test_parse_stage_errors() {
        assert!(parse_gn_utc_reply_to_stage(&[]).is_err());
        assert!(parse_gn_utc_reply_to_stage(&[3]).is_err()); // 0b11
        assert!(parse_gn_utc_reply_to_stage(&[5]).is_err()); // 0b101
        assert!(parse_gn_utc_reply_to_stage(&[1, 1]).is_err());
        assert!(parse_gn_utc_reply_to_stage(&[0]).is_err());
    }
}
