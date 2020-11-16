from typing import List

from anydex.core.transaction import Transaction, TransactionId
from anydex.core.transaction_repository import TransactionRepository


class TransactionManager:
    """Manager for retrieving and creating transactions"""

    def __init__(self, transaction_repository: TransactionRepository) -> None:
        self.transaction_repository = transaction_repository

    def find_by_id(self, transaction_id: TransactionId) -> Transaction:
        """
        :param transaction_id: The transaction id to look for
        :type transaction_id: TransactionId
        :return: The transaction or null if it cannot be found
        :rtype: Transaction
        """
        return self.transaction_repository.find_by_id(transaction_id)

    def find_all(self) -> List[Transaction]:
        return self.transaction_repository.find_all()
