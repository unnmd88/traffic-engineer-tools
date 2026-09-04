use crate::error::AsciiError;

/// Валидная ASCII строка для протокола UG-405.
///
/// Этот тип гарантирует, что строка содержит только ASCII символы
/// и не является пустой. Внутренне хранится как `String` для удобства
/// работы с текстом и эффективного доступа к байтам.
///
/// # Особенности
/// - Все конструкторы валидируют входные данные
/// - `as_str()` и `as_bytes()` работают за O(1) без аллокаций
/// - Форматирование (`to_dotted()`, `to_scn()`) создает новые строки
///
/// # Примеры
///
/// ## Создание из строки
/// ```
/// use tools_core::models::Ascii;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let s = Ascii::from_str("ABC")?;
/// assert_eq!(s.as_str(), "ABC");
/// assert_eq!(s.as_bytes(), &[65, 66, 67]);
/// # Ok(())
/// # }
/// ```
///
/// ## Создание из байт
/// ```
/// use tools_core::models::Ascii;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let s = Ascii::from_bytes(&[65, 66, 67])?;
/// assert_eq!(s.as_str(), "ABC");
/// # Ok(())
/// # }
/// ```
///
/// ## Парсинг разных форматов
/// ```
/// use tools_core::models::Ascii;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let s1 = Ascii::parse_dotted("65.66.67")?;
/// let s2 = Ascii::parse_scn(".1.3.65.66.67")?;
/// assert_eq!(s1, s2);
/// assert_eq!(s1.as_str(), "ABC");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ascii {
    s: String,
}

impl Ascii {
    /// Создает ASCII строку из обычной строки.
    ///
    /// # Валидация
    /// 1. Обрезает пробелы по краям (`.trim()`)
    /// 2. Проверяет, что строка не пустая
    /// 3. Проверяет, что все символы - ASCII
    ///
    /// # Ошибки
    /// - `AsciiError::Empty` - если после обрезки строка пустая
    /// - `AsciiError::NonAsciiCharacters` - если есть не-ASCII символы
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // Пробелы по краям будут обрезаны
    /// let s = Ascii::from_str("  CO4500  ")?;
    /// assert_eq!(s.as_str(), "CO4500");
    ///
    /// // Не-ASCII символы вызовут ошибку
    /// assert!(Ascii::from_str("Привет").is_err());
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

        Ok(Self {
            s: trimmed.to_string(),
        })
    }

    /// Создает ASCII строку из массива байт.
    ///
    /// # Валидация
    /// 1. Проверяет, что массив не пустой
    /// 2. Проверяет, что все байты - ASCII
    ///
    /// # Ошибки
    /// - `AsciiError::Empty` - если массив пустой
    /// - `AsciiError::NonAsciiCharacters` - если есть не-ASCII байты
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_bytes(b"ABC")?;
    /// assert_eq!(s.as_str(), "ABC");
    /// assert_eq!(s.as_bytes(), &[65, 66, 67]);
    ///
    /// // Управляющие символы тоже ASCII
    /// let s = Ascii::from_bytes(b"\n\tABC")?;
    /// assert_eq!(s.as_str(), "\n\tABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AsciiError> {
        // Пустой массив недопустим
        if bytes.is_empty() {
            return Err(AsciiError::Empty);
        }

        let non_ascii: Vec<char> = bytes
            .iter()
            .filter(|&&b| !b.is_ascii())
            .map(|&b| b as char)
            .collect();

        if !non_ascii.is_empty() {
            return Err(AsciiError::NonAsciiCharacters(non_ascii));
        }

        Ok(Self {
            s: String::from_utf8(bytes.to_vec()).map_err(|e| {
                tracing::error!(target: "from_bytes constructor", "{e}", );
                AsciiError::InvalidFormat
            })?,
        })
    }

    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.as_str(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_str(&self) -> &str {
        &self.s
    }

    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.as_bytes(), &[65, 66, 67]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_bytes(&self) -> &[u8] {
        self.s.as_bytes()
    }

    /// Возвращает длину строки в символах (она же длина в байтах для ASCII).
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
        self.s.len()
    }

    /// Форматирует ASCII коды через точку.
    ///
    /// Пример: `"ABC"` → `"65.66.67"`
    ///
    /// Это основной формат для протокола UG-405.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.to_dotted(), "65.66.67");
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_dotted(&self) -> String {
        // Проходим по байтам и конвертируем каждый в строку
        self.s
            .bytes() // Итератор по байтам (u8)
            .map(|b| b.to_string()) // u8 → String
            .collect::<Vec<_>>() // Собираем в Vec<String>
            .join(".") // Объединяем через точку
    }

    /// Форматирует в SCN-формат для SNMP.
    ///
    /// Формат: `.1.{длина}.{коды_через_точку}`
    ///
    /// Пример: `"ABC"` → `".1.3.65.66.67"`
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// assert_eq!(s.to_scn(), ".1.3.65.66.67");
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_scn(&self) -> String {
        // Формат: .1.{длина}.{коды}
        format!(".1.{}.{}", self.len(), self.to_dotted())
    }

    /// Форматирует коды с произвольным разделителем.
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
        self.s
            .bytes()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(delimiter)
    }

    /// Парсит строку с кодами через точку.
    ///
    /// Формат: `"65.66.67"` → `Ascii("ABC")`
    ///
    /// # Ошибки
    /// - `AsciiError::Empty` - если строка пустая
    /// - `AsciiError::InvalidFormat` - если есть пустые части (например, "65..67")
    /// - `AsciiError::InvalidCode` - если код не число или вне диапазона 0-255
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::parse_dotted("65.66.67")?;
    /// assert_eq!(s.as_str(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse_dotted(s: &str) -> Result<Self, AsciiError> {
        // Пустая строка - ошибка
        if s.is_empty() {
            return Err(AsciiError::Empty);
        }

        // Разбиваем по точке
        let parts: Vec<&str> = s.split('.').collect();
        let mut bytes = Vec::with_capacity(parts.len());

        // Парсим каждую часть как число
        for part in parts {
            // Пустая часть - ошибка формата (например, "65..67")
            if part.is_empty() {
                return Err(AsciiError::InvalidFormat);
            }

            // Парсим как u8 (0-255)
            let byte: u8 = part
                .parse()
                .map_err(|_| AsciiError::InvalidCode(part.to_string()))?;

            bytes.push(byte);
        }

        // Создаем Ascii из байт
        Self::from_bytes(&bytes)
    }

    /// Парсит SCN-формат.
    ///
    /// Формат: `.1.{длина}.{коды_через_точку}`
    ///
    /// Пример: `".1.3.65.66.67"` → `Ascii("ABC")`
    ///
    /// # Ошибки
    /// - `AsciiError::Empty` - если строка пустая
    /// - `AsciiError::InvalidFormat` - если формат не соответствует
    /// - `AsciiError::InvalidLength` - если длина не число
    /// - `AsciiError::LengthMismatch` - если длина не совпадает с количеством кодов
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::parse_scn(".1.3.65.66.67")?;
    /// assert_eq!(s.as_str(), "ABC");
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse_scn(s: &str) -> Result<Self, AsciiError> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(AsciiError::Empty);
        }

        // ".1.3.65.66.67" → ["1", "3", "65", "66", "67"]
        let parts: Vec<&str> = trimmed.trim_matches('.').split('.').collect();

        // Проверяем минимальную длину и префикс
        if parts.len() < 3 || parts[0] != "1" {
            return Err(AsciiError::InvalidFormat);
        }

        // Парсим ожидаемую длину
        let expected_len: usize = parts[1]
            .parse()
            .map_err(|_| AsciiError::InvalidLength(parts[1].to_string()))?;

        let mut bytes = Vec::with_capacity(expected_len);
        for part in &parts[2..] {
            if part.is_empty() {
                return Err(AsciiError::InvalidFormat);
            }

            let byte: u8 = part
                .parse()
                .map_err(|_| AsciiError::InvalidCode(part.to_string()))?;
            bytes.push(byte);
        }

        if bytes.len() != expected_len {
            return Err(AsciiError::LengthMismatch {
                expected: expected_len,
                actual: bytes.len(),
            });
        }

        // Создаем Ascii из байт
        Self::from_bytes(&bytes)
    }

    /// Проверяет, начинается ли строка с указанного префикса.
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
        self.s.starts_with(prefix)
    }

    /// Проверяет, содержит ли строка указанную подстроку.
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
        self.s.contains(pattern)
    }

    /// Возвращает итератор по байтам.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// let bytes: Vec<u8> = s.iter_bytes().collect();
    /// assert_eq!(bytes, vec![65, 66, 67]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter_bytes(&self) -> impl Iterator<Item = u8> + '_ {
        self.s.bytes()
    }

    /// Возвращает итератор по символам.
    ///
    /// # Пример
    /// ```
    /// use tools_core::models::Ascii;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let s = Ascii::from_str("ABC")?;
    /// let chars: Vec<char> = s.iter_chars().collect();
    /// assert_eq!(chars, vec!['A', 'B', 'C']);
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter_chars(&self) -> impl Iterator<Item = char> + '_ {
        self.s.chars()
    }
}

/// Позволяет использовать `"ABC".parse::<Ascii>()`
impl std::str::FromStr for Ascii {
    type Err = AsciiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s)
    }
}

/// Позволяет использовать `println!("{}", ascii)`
impl std::fmt::Display for Ascii {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Позволяет использовать `&[u8]` там, где ожидается Ascii
impl AsRef<[u8]> for Ascii {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Позволяет использовать `&str` там, где ожидается Ascii
impl AsRef<str> for Ascii {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Позволяет использовать `Ascii::try_from(&[65, 66, 67])`
impl TryFrom<&[u8]> for Ascii {
    type Error = AsciiError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(bytes)
    }
}

/// Позволяет использовать `Ascii::try_from(vec![65, 66, 67])`
impl TryFrom<Vec<u8>> for Ascii {
    type Error = AsciiError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(&bytes)
    }
}

/// Позволяет использовать `Ascii::try_from("ABC".to_string())`
impl TryFrom<String> for Ascii {
    type Error = AsciiError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

/// Позволяет использовать `Ascii::try_from("ABC")`
impl TryFrom<&str> for Ascii {
    type Error = AsciiError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

// ================================================================
// ТЕСТЫ
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ТЕСТЫ КОНСТРУКТОРОВ ----

    #[test]
    fn from_str_basic() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.as_str(), "ABC");
        assert_eq!(s.as_bytes(), &[65, 66, 67]);
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn from_str_with_spaces() {
        let s = Ascii::from_str("  ABC  ").unwrap();
        assert_eq!(s.as_str(), "ABC");
        assert_eq!(s.as_bytes(), &[65, 66, 67]);
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
    fn from_bytes_basic() {
        let s = Ascii::from_bytes(b"ABC").unwrap();
        assert_eq!(s.as_str(), "ABC");
        assert_eq!(s.as_bytes(), &[65, 66, 67]);
    }

    #[test]
    fn from_bytes_empty() {
        let err = Ascii::from_bytes(b"").unwrap_err();
        assert!(matches!(err, AsciiError::Empty));
    }

    #[test]
    fn from_bytes_non_ascii() {
        let err = Ascii::from_bytes(&[65, 66, 208, 184]).unwrap_err();
        match err {
            AsciiError::NonAsciiCharacters(chars) => {
                assert_eq!(chars, vec!['и']);
            }
            _ => panic!("Expected NonAsciiCharacters"),
        }
    }

    #[test]
    fn from_bytes_control_chars() {
        let s = Ascii::from_bytes(b"\n\tABC").unwrap();
        assert_eq!(s.as_str(), "\n\tABC");
        assert_eq!(s.as_bytes(), &[10, 9, 65, 66, 67]);
    }

    // ---- ТЕСТЫ ФОРМАТТЕРОВ ----

    #[test]
    fn to_dotted() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_dotted(), "65.66.67");
    }

    #[test]
    fn to_scn() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_scn(), ".1.3.65.66.67");
    }

    #[test]
    fn to_scn_complex() {
        let s = Ascii::from_str("CO4500").unwrap();
        assert_eq!(s.to_scn(), ".1.6.67.79.52.53.48.48");
    }

    #[test]
    fn to_delimited() {
        let s = Ascii::from_str("ABC").unwrap();
        assert_eq!(s.to_delimited(","), "65,66,67");
        assert_eq!(s.to_delimited("-"), "65-66-67");
        assert_eq!(s.to_delimited(""), "656667");
    }

    // ---- ТЕСТЫ ПАРСЕРОВ ----

    #[test]
    fn parse_dotted_basic() {
        let s = Ascii::parse_dotted("65.66.67").unwrap();
        assert_eq!(s.as_str(), "ABC");
        assert_eq!(s.as_bytes(), &[65, 66, 67]);
    }

    #[test]
    fn parse_dotted_single() {
        let s = Ascii::parse_dotted("65").unwrap();
        assert_eq!(s.as_str(), "A");
        assert_eq!(s.as_bytes(), &[65]);
    }

    #[test]
    fn parse_dotted_empty() {
        let err = Ascii::parse_dotted("").unwrap_err();
        assert!(matches!(err, AsciiError::Empty));
    }

    #[test]
    fn parse_dotted_invalid() {
        let err = Ascii::parse_dotted("65.abc.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidCode(_)));
    }

    #[test]
    fn parse_dotted_empty_parts() {
        let err = Ascii::parse_dotted("65..67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidFormat));
    }

    #[test]
    fn parse_dotted_out_of_range() {
        let err = Ascii::parse_dotted("65.256.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidCode(_)));
    }

    #[test]
    fn parse_scn_basic() {
        let s = Ascii::parse_scn(".1.3.65.66.67").unwrap();
        assert_eq!(s.as_str(), "ABC");
        assert_eq!(s.as_bytes(), &[65, 66, 67]);
    }

    #[test]
    fn parse_scn_complex() {
        let s = Ascii::parse_scn(".1.6.67.79.52.53.48.48").unwrap();
        assert_eq!(s.as_str(), "CO4500");
        assert_eq!(s.as_bytes(), &[67, 79, 52, 53, 48, 48]);
    }

    #[test]
    fn parse_scn_single() {
        let s = Ascii::parse_scn(".1.1.65").unwrap();
        assert_eq!(s.as_str(), "A");
        assert_eq!(s.as_bytes(), &[65]);
    }

    #[test]
    fn parse_scn_invalid_format() {
        let err = Ascii::parse_scn("invalid").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidFormat));
    }

    #[test]
    fn parse_scn_invalid_prefix() {
        let err = Ascii::parse_scn(".2.3.65.66.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidFormat));
    }

    #[test]
    fn parse_scn_invalid_length() {
        let err = Ascii::parse_scn(".1.abc.65.66.67").unwrap_err();
        assert!(matches!(err, AsciiError::InvalidLength(_)));
    }
}
