use std::cmp;

use rand::Rand;
use serde::{de::DeserializeOwned, Serialize};

use super::{Batch, Change, ChangeState, DynamicHoneyBadger, Epoch, Message, Step};
use sender_queue::SenderQueueFunc;
use {Contribution, Epoched, NodeIdT};

impl<C, N> SenderQueueFunc<DynamicHoneyBadger<C, N>> for DynamicHoneyBadger<C, N>
where
    C: Contribution + Serialize + DeserializeOwned,
    N: NodeIdT + Serialize + DeserializeOwned + Rand,
{
    type Step = Step<C, N>;

    fn max_epoch_with_batch(&self, epoch: Epoch, batch: &Batch<C, N>) -> (Epoch, Option<N>) {
        (
            cmp::max(batch.next_epoch, epoch),
            if let ChangeState::InProgress(Change::Add(ref id, _)) = batch.change {
                // Register the new node to send broadcast messages to it from now on.
                Some(id.clone())
            } else {
                None
            },
        )
    }

    fn is_accepting_epoch(&self, us: &Message<N>, Epoch((them_era, them_hb_epoch)): Epoch) -> bool {
        let Epoch((era, hb_epoch)) = us.epoch();
        era == them_era
            && (them_hb_epoch <= hb_epoch
                && hb_epoch <= them_hb_epoch.map(|e| e + self.max_future_epochs as u64))
    }

    fn is_later_epoch(&self, us: &Message<N>, Epoch((them_era, them_hb_epoch)): Epoch) -> bool {
        let Epoch((era, hb_epoch)) = us.epoch();
        era < them_era || (era == them_era && hb_epoch.is_some() && hb_epoch < them_hb_epoch)
    }

    fn spanning_epochs(epoch: Epoch) -> Vec<Epoch> {
        if let Epoch((era, Some(_))) = epoch {
            vec![Epoch((era, None))]
        } else {
            vec![]
        }
    }
}
