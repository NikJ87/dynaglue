import {
  Delete,
  Put,
  TransactWriteItem,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { CompositeCondition } from '../base/conditions';
import { InvalidFindDescriptorException } from '../base/exceptions';
import { Context } from '../context';
import debugDynamo from '../debug/debugDynamo';
import { createDeleteByIdRequest } from './delete_by_id';
import { createReplaceByIdRequest } from './replace';

export type TransactionReplaceRequest = {
  collectionName: string;
  value: Record<string, unknown>;
  options?: { condition?: CompositeCondition };
};

export type TransactionDeleteRequest = {
  collectionName: string;
  id: string;
  options?: { condition?: CompositeCondition };
};

export type TransactionWriteRequest =
  | TransactionReplaceRequest
  | TransactionDeleteRequest;

const isTransactionReplaceRequest = (
  transactionWriteRequest: TransactionWriteRequest
): transactionWriteRequest is TransactionReplaceRequest =>
  'value' in transactionWriteRequest && !!transactionWriteRequest.value;

const isTransactionDeleteRequest = (
  transactionWriteRequest: TransactionDeleteRequest
): transactionWriteRequest is TransactionDeleteRequest =>
  'id' in transactionWriteRequest && !!transactionWriteRequest.id;

export const transactionWrite = async (
  context: Context,
  transactionWriteRequests: TransactionWriteRequest[]
): Promise<void> => {
  if (!transactionWriteRequests || transactionWriteRequests.length === 0) {
    throw new InvalidFindDescriptorException(
      'At least one request should be provided'
    );
  } else if (transactionWriteRequests.length > 25) {
    throw new InvalidFindDescriptorException(
      'No more than 25 requests can be specified to transactionWrite'
    );
  }
  const transactWriteItem: TransactWriteItem[] = transactionWriteRequests.map(
    (request) => {
      if (isTransactionReplaceRequest(request)) {
        const { collectionName, value, options } = request;
        const { request: putItemInput } = createReplaceByIdRequest(
          context,
          collectionName,
          value,
          options
        );

        return { Put: putItemInput } as { Put: Put };
      }

      if (isTransactionDeleteRequest(request)) {
        const { collectionName, id, options } = request;

        const deleteItem = createDeleteByIdRequest(
          context,
          collectionName,
          id,
          options
        );

        return { Delete: deleteItem } as { Delete: Delete };
      }
    }
  ) as unknown as TransactWriteItem[];

  try {
    const request = { TransactItems: transactWriteItem };

    debugDynamo('transactWriteItems', JSON.stringify(request));
    console.log('transactWriteItems : ', JSON.stringify(request));

    const command = new TransactWriteItemsCommand(request);
    const response = await context.ddb.send(command);

    console.log(
      'response of writing to DDB by transaction : ',
      JSON.stringify(response)
    );
  } catch (error) {
    console.error('error in transaction by id : ', error);
  }
};
