use tools_core::monitor::application::{AppConfig, Application};

pub struct AppBuilder;

impl AppBuilder {
    pub async fn from_yaml(content: &str) -> anyhow::Result<Application> {
        let app_cfg: AppConfig = serde_yaml::from_str(content)?;
        Ok(Application::new(app_cfg).await?)
    }
}
