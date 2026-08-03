use crate::error::AsciiError;

/// Валидная ASCII строка для протокола UG-405.
///
/// Этот тип представляет собой ASCII-строку, которая может быть представлена
/// в нескольких форматах:
/// - Человекочитаемая строка (`as_string()`)
/// - Массив ASCII кодов (`codes()`)
/// - Коды через точку (`delimited_codes()`)
/// - SCN-формат для SNMP (`scn()`)
///
/// Все представления вычисляются при создании и кэшируются.
///
/// # Примеры
///
/// ## Создание из строки
/// ```
/// use tools_core::models::Ascii;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let s = Ascii::from_str("ABC")?;
/// assert_eq!(s.as_string(), "ABC");
/// assert_eq!(s.codes(), &[65, 66, 67]);
/// assert_eq!(s.delimited_codes(), "65.66.67");
/// assert_eq!(s.scn(), ".1.3.65.66.67");
/// # Ok(())
/// # }
/// ```
///
/// ## Создание из кодов через точку
/// ```
/// use tools_core::models::Ascii;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let s = Ascii::from_codes("65.66.67")?;
/// assert_eq!(s.as_string(), "ABC");
/// # Ok(())
/// # }
/// ```
///
/// ## Создание из SCN-формата
/// ```
/// use tools_core::models::Ascii;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let s = Ascii::from_scn(".1.3.65.66.67")?;
/// assert_eq!(s.as_string(), "ABC");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ascii {
    /// Человекочитаемая строка.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.as_string(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    as_string: String,

    /// ASCII коды каждого символа.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.codes(), &[65, 66, 67]);
    /// # Ok(())
    /// # }
    /// ```
    codes: Vec<u8>,

    /// Коды, разделённые точкой (основной формат для протокола).
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.delimited_codes(), "65.66.67");
    /// # Ok(())
    /// # }
    /// ```
    delimited_codes: String,

    /// Полный SCN-формат: `.1.{длина}.{коды_через_точку}`
    ///
    /// Используется в SNMP-запросах как часть OID.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.scn(), ".1.3.65.66.67");
    /// # Ok(())
    /// # }
    /// ```
    scn: String,
}

impl Ascii {
    /// Создать из обычной строки.
    ///
    /// # Валидация
    /// - Обрезает пробелы по краям
    /// - Проверяет, что строка не пустая
    /// - Проверяет, что все символы — ASCII
    ///
    /// # Ошибки
    /// - `AsciiError::Empty` — если строка пустая
    /// - `AsciiError::NonAsciiCharacters` — если есть не-ASCII символы
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("CO4500")?;
    /// assert_eq!(s.as_string(), "CO4500");
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_str(s: &str) -> Result<Self, AsciiError> {
        let trimmed = s.trim();

        if trimmed.is_empty() {
            return Err(AsciiError::Empty);
        }

        let non_ascii: Vec<char> = trimmed.chars().filter(|c| !c.is_ascii()).collect();

        if !non_ascii.is_empty() {
            return Err(AsciiError::NonAsciiCharacters(non_ascii));
        }

        let decoded = trimmed.to_string();
        let codes: Vec<u8> = trimmed.bytes().collect();
        let dotted = codes
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(".");
        let scn = format!(".1.{}.{}", codes.len(), dotted);

        Ok(Self {
            as_string: decoded,
            codes,
            delimited_codes: dotted,
            scn,
        })
    }

    /// Создать из кодов, разделённых точкой.
    ///
    /// # Формат
    /// Коды должны быть разделены точкой: `"65.66.67"`
    ///
    /// # Ошибки
    /// - `AsciiError::Empty` — если строка пустая
    /// - `AsciiError::InvalidCode` — если код не число
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_codes("65.66.67")?;
    /// assert_eq!(s.as_string(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_codes(s: &str) -> Result<Self, AsciiError> {
        let parts: Vec<&str> = s.split('.').collect();

        if parts.is_empty() {
            return Err(AsciiError::Empty);
        }

        let mut codes = Vec::new();
        for part in parts {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }

            let code: u8 = trimmed
                .parse()
                .map_err(|_| AsciiError::InvalidCode(trimmed.to_string()))?;

            codes.push(code);
        }

        if codes.is_empty() {
            return Err(AsciiError::Empty);
        }

        let decoded = codes.iter().map(|&b| b as char).collect::<String>();
        let dotted = codes
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(".");
        let scn = format!(".1.{}.{}", codes.len(), dotted);

        Ok(Self {
            as_string: decoded,
            codes,
            delimited_codes: dotted,
            scn,
        })
    }

    /// Создать из SCN-формата.
    ///
    /// # Формат
    /// `.1.{длина}.{коды_через_точку}`
    ///
    /// # Ошибки
    /// - `AsciiError::InvalidFormat` — если формат не соответствует
    /// - `AsciiError::InvalidPrefix` — если префикс не `1`
    /// - `AsciiError::InvalidLength` — если длина не число
    /// - `AsciiError::LengthMismatch` — если длина не совпадает с количеством кодов
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_scn(".1.3.65.66.67")?;
    /// assert_eq!(s.as_string(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_scn(s: &str) -> Result<Self, AsciiError> {
        let parts: Vec<&str> = s.trim_matches('.').split('.').collect();

        if parts.len() < 3 || parts[0] != "1" {
            return Err(AsciiError::InvalidFormat);
        }

        let expected_len: usize = parts[1]
            .parse()
            .map_err(|_| AsciiError::InvalidLength(parts[1].to_string()))?;

        let codes_str = parts[2..].join(".");
        let data = Self::from_codes(&codes_str)?;

        if data.codes.len() != expected_len {
            return Err(AsciiError::LengthMismatch {
                expected: expected_len,
                actual: data.codes.len(),
            });
        }

        Ok(data)
    }

    /// Получить человекочитаемую строку.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.as_string(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_string(&self) -> &str {
        &self.as_string
    }

    /// Получить ASCII коды.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.codes(), &[65, 66, 67]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn codes(&self) -> &[u8] {
        &self.codes
    }

    /// Получить коды, разделённые точкой.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.delimited_codes(), "65.66.67");
    /// # Ok(())
    /// # }
    /// ```
    pub fn delimited_codes(&self) -> &str {
        &self.delimited_codes
    }

    /// Получить SCN-формат.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.scn(), ".1.3.65.66.67");
    /// # Ok(())
    /// # }
    /// ```
    pub fn scn(&self) -> &str {
        &self.scn
    }

    /// Получить длину строки.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn len(&self) -> usize {
        self.codes.len()
    }

    /// Получить коды с любым разделителем.
    ///
    /// Для точки (`.`) — просто возвращает готовое значение.
    /// Для других разделителей — вычисляет.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.to_delimited(","), "65,66,67");
    /// assert_eq!(s.to_delimited("-"), "65-66-67");
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_delimited(&self, delimiter: &str) -> String {
        if delimiter == "." {
            self.delimited_codes.clone()
        } else {
            self.codes
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(delimiter)
        }
    }

    /// Проверить, начинается ли строка с префикса.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert!(s.starts_with("A"));
    /// assert!(!s.starts_with("B"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn starts_with(&self, prefix: &str) -> bool {
        self.as_string.starts_with(prefix)
    }

    /// Проверить, содержит ли строка подстроку.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert!(s.contains("B"));
    /// assert!(!s.contains("D"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn contains(&self, pattern: &str) -> bool {
        self.as_string.contains(pattern)
    }
}

// ================================================================
// ТЕСТЫ
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_basic() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.as_string(), "ABC");
        assert_eq!(s.codes(), &[65, 66, 67]);
        assert_eq!(s.delimited_codes(), "65.66.67");
        assert_eq!(s.scn(), ".1.3.65.66.67");
    }

    #[test]
    fn from_str_with_spaces() {
        let s = Ascii::from_str("  ABC  ").unwrap();
        assert_eq!(s.as_string(), "ABC");
        assert_eq!(s.codes(), &[65, 66, 67]);
    }

    #[test]
    fn from_str_lowercase() {
        let s = Ascii::from_str("abc").unwrap();
        assert_eq!(s.as_string(), "abc");
        assert_eq!(s.codes(), &[97, 98, 99]);
        assert_eq!(s.delimited_codes(), "97.98.99");
        assert_eq!(s.scn(), ".1.3.97.98.99");
    }

    #[test]
    fn from_str_complex() {
        let s = Ascii::from_str("CO4500").unwrap();
        assert_eq!(s.as_string(), "CO4500");
        assert_eq!(s.codes(), &[67, 79, 52, 53, 48, 48]);
        assert_eq!(s.delimited_codes(), "67.79.52.53.48.48");
        assert_eq!(s.scn(), ".1.6.67.79.52.53.48.48");
    }

    #[test]
    fn from_str_empty() {
        let err = Ascii::from_str("").unwrap_err();
        assert!(matches!(err, AsciiError::Empty));
    }

    #[test]
    fn from_str_whitespace_only() {
        let err = Ascii::from_str("   ").unwrap_err();
        assert!(matches!(err, AsciiError::Empty));
    }

    #[test]
    fn from_str_non_ascii() {
        let err = Ascii::from_str("Привет").unwrap_err();
        match err {
            AsciiError::NonAsciiCharacters(chars) => {
                assert_eq!(chars, vec!['П', 'р', 'и', 'в', 'е', 'т']);
            }
            _ => panic!("Expected NonAsciiCharacters"),
        }
    }

    #[test]
    fn from_str_mixed_ascii_non_ascii() {
        let err = Ascii::from_str("ABCПривет").unwrap_err();
        match err {
            AsciiError::NonAsciiCharacters(chars) => {
                assert_eq!(chars, vec!['П', 'р', 'и', 'в', 'е', 'т']);
            }
            _ => panic!("Expected NonAsciiCharacters"),
        }
    }

    #[test]
    fn from_codes_basic() {
        let s = Ascii::from_codes("65.66.67").unwrap();
        assert_eq!(s.as_string(), "ABC");
        assert_eq!(s.codes(), &[65, 66, 67]);
        assert_eq!(s.delimited_codes(), "65.66.67");
        assert_eq!(s.scn(), ".1.3.65.66.67");
    }

    #[test]
    fn from_codes_single() {
        let s = Ascii::from_codes("65").unwrap();
        assert_eq!(s.as_string(), "A");
        assert_eq!(s.codes(), &[65]);
        assert_eq!(s.delimited_codes(), "65");
        assert_eq!(s.scn(), ".1.1.65");
    }

    #[test]
    fn from_codes_with_spaces() {
        let s = Ascii::from_codes("65 . 66 . 67").unwrap();
        assert_eq!(s.as_string(), "ABC");
        assert_eq!(s.codes(), &[65, 66, 67]);
    }

    #[test]
    fn from_codes_empty() {
        let err = Ascii::from_codes("").unwrap_err();
        assert!(matches!(err, AsciiError::Empty));
    }

    #[test]
    fn from_codes_invalid() {
        let err = Ascii::from_codes("65.abc.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidCode(_)));
    }

    #[test]
    fn from_codes_out_of_range() {
        let err = Ascii::from_codes("65.256.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidCode(_)));
    }

    #[test]
    fn from_scn_basic() {
        let s = Ascii::from_scn(".1.3.65.66.67").unwrap();
        assert_eq!(s.as_string(), "ABC");
        assert_eq!(s.codes(), &[65, 66, 67]);
        assert_eq!(s.delimited_codes(), "65.66.67");
        assert_eq!(s.scn(), ".1.3.65.66.67");
    }

    #[test]
    fn from_scn_complex() {
        let s = Ascii::from_scn(".1.6.67.79.52.53.48.48").unwrap();
        assert_eq!(s.as_string(), "CO4500");
        assert_eq!(s.codes(), &[67, 79, 52, 53, 48, 48]);
    }

    #[test]
    fn from_scn_single() {
        let s = Ascii::from_scn(".1.1.65").unwrap();
        assert_eq!(s.as_string(), "A");
        assert_eq!(s.codes(), &[65]);
    }

    #[test]
    fn from_scn_invalid_format() {
        let err = Ascii::from_scn("invalid").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidFormat));
    }

    #[test]
    fn from_scn_invalid_prefix() {
        let err = Ascii::from_scn(".2.3.65.66.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidFormat));
    }

    #[test]
    fn from_scn_invalid_length() {
        let err = Ascii::from_scn(".1.abc.65.66.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidLength(_)));
    }

    #[test]
    fn from_scn_length_mismatch() {
        let err = Ascii::from_scn(".1.2.65.66.67").unwrap_err();
        match err {
            AsciiError::LengthMismatch { expected, actual } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 3);
            }
            _ => panic!("Expected LengthMismatch"),
        }
    }

    #[test]
    fn to_delimited_default() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_delimited("."), "65.66.67");
    }

    #[test]
    fn to_delimited_comma() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_delimited(","), "65,66,67");
    }

    #[test]
    fn to_delimited_dash() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_delimited("-"), "65-66-67");
    }

    #[test]
    fn to_delimited_empty() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_delimited(""), "656667");
    }

    #[test]
    fn len() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn starts_with() {
        let s = Ascii::from_str("ABC").unwrap();
        assert!(s.starts_with("A"));
        assert!(s.starts_with("AB"));
        assert!(!s.starts_with("B"));
        assert!(!s.starts_with("ABCD"));
    }

    #[test]
    fn contains() {
        let s = Ascii::from_str("ABC").unwrap();
        assert!(s.contains("A"));
        assert!(s.contains("B"));
        assert!(s.contains("C"));
        assert!(s.contains("AB"));
        assert!(s.contains("BC"));
        assert!(!s.contains("D"));
        assert!(!s.contains("ABCD"));
    }

    #[test]
    fn clone() {
        let s1 = Ascii::from_str("ABC").unwrap();
        let s2 = s1.clone();
        assert_eq!(s1, s2);
    }

    #[test]
    fn debug() {
        let s = Ascii::from_str("ABC").unwrap();
        let debug = format!("{:?}", s);
        assert!(debug.contains("ABC"));
    }

    #[test]
    fn partial_eq() {
        let s1 = Ascii::from_str("ABC").unwrap();
        let s2 = Ascii::from_str("ABC").unwrap();
        let s3 = Ascii::from_str("DEF").unwrap();
        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn eq_with_different_constructors() {
        let s1 = Ascii::from_str("ABC").unwrap();
        let s2 = Ascii::from_codes("65.66.67").unwrap();
        let s3 = Ascii::from_scn(".1.3.65.66.67").unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s1, s3);
    }
}
