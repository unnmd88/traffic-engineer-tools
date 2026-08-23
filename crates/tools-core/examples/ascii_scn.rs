use tools_core::domain::ascii::Ascii;

fn main() -> anyhow::Result<()> {
    let engine = Ascii::from_str("CO101")?;
    println!("{}", engine.as_str());
    println!("{}", engine.to_scn());
    println!("{:?}", engine.as_bytes());

    println!("{}", "-".repeat(40));

    let bytes_co101 = [67, 79, 49, 48, 49];
    let co101 = Ascii::from_bytes(&bytes_co101)?;
    println!("{}", co101.as_str());

    Ok(())
}
