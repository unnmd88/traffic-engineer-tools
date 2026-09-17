mod metadata;
pub use metadata::*;

/// Текущая фаза (read): статус, который отдаёт контроллер.
pub const STAGE_ALIASES: &[&str] = &["stage", "фаза", "current_stage"];
/// Команда/форсирование фазы (write): OID, которым фазу устанавливают.
pub const SET_STAGE_ALIASES: &[&str] = &["set_stage", "set_phase", "force_stage", "phase_command"];
pub const TO_BIT_ALIASES: &[&str] = &["to", " to_bit", "to bit"];
pub const OPERATION_MODE_ALIASES: &[&str] = &["operation mode", "operation_mode", "mode", "opmode"];
