use crossterm::{
    cursor, execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
    terminal::{Clear, ClearType},
};
use std::io::stdout;
use tools_core::monitor::Snapshot;

pub fn render_snapshot(snapshot: &Snapshot) -> anyhow::Result<()> {
    execute!(stdout(), Clear(ClearType::All), cursor::MoveTo(0, 0))?;

    println!("{}", snapshot);

    Ok(())
}
