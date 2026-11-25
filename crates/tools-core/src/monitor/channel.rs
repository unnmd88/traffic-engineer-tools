use tokio::sync::{
    mpsc::{
        self,
        error::{SendError, TrySendError},
    },
    watch,
};

use super::ApplicationId;

pub(super) struct ChannelSender<T> {
    sender: mpsc::Sender<T>,
    saturation: Option<Saturation>,
}

#[derive(Clone)]
struct Saturation {
    application_id: ApplicationId,
    full: watch::Sender<bool>,
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            saturation: self.saturation.clone(),
        }
    }
}

impl<T> From<mpsc::Sender<T>> for ChannelSender<T> {
    fn from(sender: mpsc::Sender<T>) -> Self {
        Self {
            sender,
            saturation: None,
        }
    }
}

impl<T> ChannelSender<T> {
    pub(super) fn monitored(
        sender: mpsc::Sender<T>,
        application_id: ApplicationId,
        full: watch::Sender<bool>,
    ) -> Self {
        Self {
            sender,
            saturation: Some(Saturation {
                application_id,
                full,
            }),
        }
    }

    pub(super) async fn send(&self, message: impl Into<T>) -> Result<(), SendError<()>> {
        self.reserve().await?.send(message.into());
        Ok(())
    }

    pub(super) async fn send_while_running(
        &self,
        message: impl Into<T>,
    ) -> Result<(), SendError<()>> {
        self.reserve_while_running().await?.send(message.into());
        Ok(())
    }

    pub(super) async fn reserve(&self) -> Result<mpsc::Permit<'_, T>, SendError<()>> {
        if let Some(saturation) = &self.saturation {
            match self.try_reserve(saturation) {
                Ok(permit) => return Ok(permit),
                Err(TrySendError::Closed(())) => return Err(SendError(())),
                Err(TrySendError::Full(())) => {}
            }
        }
        self.sender.reserve().await
    }

    /// Rejects new work on saturation, leaving reliable reservations available for results.
    pub(super) async fn reserve_while_running(&self) -> Result<mpsc::Permit<'_, T>, SendError<()>> {
        match &self.saturation {
            Some(saturation) if *saturation.full.borrow() => Err(SendError(())),
            Some(saturation) => self.try_reserve(saturation).map_err(|_| SendError(())),
            None => self.sender.reserve().await,
        }
    }

    fn try_reserve(
        &self,
        saturation: &Saturation,
    ) -> Result<mpsc::Permit<'_, T>, TrySendError<()>> {
        let result = self.sender.try_reserve();
        if matches!(result, Err(TrySendError::Full(())))
            && saturation
                .full
                .send_if_modified(|full| !std::mem::replace(full, true))
        {
            tracing::error!(
                application_id = %saturation.application_id,
                capacity = self.sender.max_capacity(),
                "ordered monitor queue is full; shutting down"
            );
        }
        result
    }

    pub(super) async fn closed(&self) {
        self.sender.closed().await;
    }

    #[cfg(test)]
    pub(super) fn raw(&self) -> &mpsc::Sender<T> {
        &self.sender
    }
}
