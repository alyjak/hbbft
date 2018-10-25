use rand::Rand;
use serde::{de::DeserializeOwned, Serialize};

use super::{Batch, DynamicHoneyBadger, Epoch, Message, QueueingHoneyBadger, Step};
use sender_queue::SenderQueueFunc;
use transaction_queue::TransactionQueue;
use {Contribution, NodeIdT};

impl<T, N, Q> SenderQueueFunc<QueueingHoneyBadger<T, N, Q>> for QueueingHoneyBadger<T, N, Q>
where
    T: Contribution + Serialize + DeserializeOwned + Clone,
    N: NodeIdT + Serialize + DeserializeOwned + Rand,
    Q: TransactionQueue<T>,
{
    type Step = Step<T, N, Q>;

    fn max_epoch_with_batch(&self, epoch: Epoch, batch: &Batch<T, N>) -> (Epoch, Option<N>) {
        self.dyn_hb.max_epoch_with_batch(epoch, batch)
    }

    fn is_accepting_epoch(&self, us: &Message<N>, them: Epoch) -> bool {
        self.dyn_hb.is_accepting_epoch(us, them)
    }

    fn is_later_epoch(&self, us: &Message<N>, them: Epoch) -> bool {
        self.dyn_hb.is_later_epoch(us, them)
    }

    fn spanning_epochs(epoch: Epoch) -> Vec<Epoch> {
        <DynamicHoneyBadger<Vec<T>, N> as SenderQueueFunc<DynamicHoneyBadger<Vec<T>, N>>>
            ::spanning_epochs(epoch)
    }
}
