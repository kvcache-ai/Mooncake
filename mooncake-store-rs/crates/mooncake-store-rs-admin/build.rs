fn main() -> Result<(), Box<dyn std::error::Error>> {
    shadow_rs::ShadowBuilder::builder().build()?;
    Ok(())
}
