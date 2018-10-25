use rand::{Rand, Rng};

use Epoched;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MessageContent<M> {
    EpochStarted,
    Algo(M),
}

impl<M> Rand for MessageContent<M>
where
    M: Epoched + Rand,
{
    fn rand<R: Rng>(rng: &mut R) -> Self {
        let message_type = *rng.choose(&["epoch", "algo"]).unwrap();

        match message_type {
            "epoch" => MessageContent::EpochStarted,
            "algo" => MessageContent::Algo(rng.gen::<M>()),
            _ => unreachable!(),
        }
    }
}

impl<M> MessageContent<M>
where
    M: Epoched,
{
    pub fn with_epoch(self, epoch: <M as Epoched>::Epoch) -> Message<M> {
        Message {
            epoch,
            content: self,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message<M>
where
    M: Epoched,
{
    pub epoch: <M as Epoched>::Epoch,
    pub content: MessageContent<M>,
}

impl<M: Epoched> Epoched for Message<M> {
    type Epoch = <M as Epoched>::Epoch;

    fn epoch(&self) -> Self::Epoch {
        self.epoch
    }
}

impl<M> From<M> for Message<M>
where
    M: Epoched,
{
    fn from(message: M) -> Self {
        Message {
            epoch: message.epoch(),
            content: MessageContent::Algo(message),
        }
    }
}

impl<M> Rand for Message<M>
where
    M: Epoched + Rand,
    <M as Epoched>::Epoch: Rand,
{
    fn rand<R: Rng>(rng: &mut R) -> Self {
        Message {
            epoch: rng.gen::<<M as Epoched>::Epoch>(),
            content: rng.gen::<MessageContent<M>>(),
        }
    }
}
