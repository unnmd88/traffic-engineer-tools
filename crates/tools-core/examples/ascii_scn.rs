use tools_core::ascii::Ascii;

fn main() -> anyhow::Result<()> {
    let engine = Ascii::from_str("CO101")?;
    println!("{}", engine.as_str());
    println!("scn_index: {}", engine.to_scn_index());
    println!("utc_index: {}", engine.to_utc_index());
    println!("{:?}", engine.as_bytes());

    println!("{}", "-".repeat(40));

    let bytes_co101 = [67, 79, 49, 48, 49];
    let co101 = Ascii::from_bytes(&bytes_co101)?;
    println!("{}", co101.as_str());

    Ok(())
}
