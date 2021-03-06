import logging

from anydex.core.payment import Payment
from anydex.core.timestamp import Timestamp
from anydex.core.transaction import Transaction
from anydex.core.transaction_repository import TransactionRepository


class TransactionManager(object):
    """Manager for retrieving and creating transactions"""

    def __init__(self, transaction_repository):
        """
        :type transaction_repository: TransactionRepository
        """
        super(TransactionManager, self).__init__()

        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info("Transaction manager initialized")

        self.transaction_repository = transaction_repository

    def create_from_proposed_trade(self, proposed_trade):
        """
        :type proposed_trade: ProposedTrade
        :rtype: Transaction
        """
        transaction = Transaction.from_proposed_trade(proposed_trade, self.transaction_repository.next_identity())
        self.transaction_repository.add(transaction)

        self._logger.info("Transaction created with id: %s, asset pair %s",
                          transaction.transaction_id, transaction.assets)
        return transaction

    def create_from_start_transaction(self, start_transaction):
        """
        :type start_transaction: StartTransaction
        :rtype: Transaction
        """
        transaction = Transaction(start_transaction.transaction_id, start_transaction.assets,
                                  start_transaction.recipient_order_id, start_transaction.order_id, Timestamp.now())
        self.transaction_repository.add(transaction)

        self._logger.info("Transaction created with id: %s, asset pair %s",
                          transaction.transaction_id, transaction.assets)

        return transaction

    def create_payment_message(self, message_id, payment_id, transaction, transferred_assets, success):
        payment_message = Payment(message_id, transaction.transaction_id, transferred_assets,
                                  transaction.outgoing_address, transaction.partner_incoming_address,
                                  payment_id, Timestamp.now(), success)
        transaction.add_payment(payment_message)
        self.transaction_repository.update(transaction)

        return payment_message

    def find_by_id(self, transaction_id):
        """
        :param transaction_id: The transaction id to look for
        :type transaction_id: TransactionId
        :return: The transaction or null if it cannot be found
        :rtype: Transaction
        """
        return self.transaction_repository.find_by_id(transaction_id)

    def find_all(self):
        """
        :rtype: [Transaction]
        """
        return self.transaction_repository.find_all()
