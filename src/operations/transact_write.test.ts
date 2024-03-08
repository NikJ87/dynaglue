import {
  CreateTableCommand,
  CreateTableInput,
  DeleteTableCommand,
  DynamoDBClient,
  ListTablesCommand,
} from '@aws-sdk/client-dynamodb';
import { CollectionLayout } from '../base/layout';
import { createContext } from '../context';
import { replace } from './replace';
import {
  TransactFindByIdDescriptor,
  transactFindByIds,
} from './transact_find_by_ids';
import { TransactionWriteRequest, transactionWrite } from './transact_write';

const TableDefinitions = [
  {
    TableName: 'User',
    KeySchema: [
      { AttributeName: 'pk', KeyType: 'HASH' },
      { AttributeName: 'sk', KeyType: 'RANGE' },
    ],
    AttributeDefinitions: [
      { AttributeName: 'pk', AttributeType: 'S' },
      { AttributeName: 'sk', AttributeType: 'S' },
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  },
];

const showTimeTaken = (startTime: number) =>
  `[${new Date().getTime() - startTime}ms]`;

const LocalDDBTestKit = {
  connect: (): DynamoDBClient | null => {
    const startBy = new Date().getTime();
    try {
      const localDDBClient = new DynamoDBClient({
        endpoint: 'http://localhost:8000',
        region: 'local',
      });
      console.info(`${showTimeTaken(startBy)} Connected to Local DDB`);
      return localDDBClient;
    } catch (error) {
      console.error('Error connecting to local DDB');
      return null;
    }
  },
  createTables: async (
    client: DynamoDBClient,
    tableDefinitions: CreateTableInput[] = []
  ) => {
    const startBy = new Date().getTime();
    try {
      await Promise.all(
        tableDefinitions?.map((tableDefinition) => {
          const createTableCmd = new CreateTableCommand(tableDefinition);
          return client.send(createTableCmd);
        })
      );

      console.log(`${showTimeTaken(startBy)} tables created in local DDB`);
    } catch (error) {
      console.error('Error creating tables in local DDB');
    }
  },
  deleteTables: async (client: DynamoDBClient, tableNames: string[] = []) => {
    const startBy = new Date().getTime();
    try {
      await Promise.all(
        tableNames?.map((tableName) => {
          return client.send(
            new DeleteTableCommand({
              TableName: tableName,
            })
          );
        })
      );

      console.log(`${showTimeTaken(startBy)} tables deleted in local DDB`);
    } catch (error) {
      console.error('Error deleting tables in local DDB');
    }
  },
  listTables: async (client: DynamoDBClient) => {
    const startBy = new Date().getTime();
    try {
      const listTables = await client.send(new ListTablesCommand({}));
      console.log(
        `${showTimeTaken(startBy)} tables in local DDB : `,
        listTables
      );
    } catch (error) {
      console.error('Error listing tables in local DDB');
    }
  },
};

describe('transactions', () => {
  /**
   * ToDo probably using script we can manage it
   */
  // beforeAll(startDb, 10000);
  // afterAll(stopDb, 5000);

  let localDDBClient: DynamoDBClient;

  // create a client with DDB local
  beforeAll(async () => {
    localDDBClient = LocalDDBTestKit.connect() as unknown as DynamoDBClient;
  });

  // create tables
  beforeAll(async () => {
    await LocalDDBTestKit.createTables(localDDBClient, TableDefinitions);
  });

  // Delete tables
  afterAll(async () => {
    await LocalDDBTestKit.deleteTables(localDDBClient, [
      TableDefinitions[0].TableName,
    ]);
  });

  const layout: CollectionLayout = {
    tableName: 'User',
    primaryKey: { partitionKey: 'pk', sortKey: 'sk' },
  };
  const collection = {
    name: 'users',
    layout,
  };

  test.each([
    { _id: 'test-id', name: 'Moriarty', email: 'moriarty@jim.com' },
    {
      _id: 'test-sh',
      name: 'Sherlock',
      email: 'sh@sh.com',
    },
  ])('Insert items to the collection using replace', async (value) => {
    const context = createContext(localDDBClient as unknown as DynamoDBClient, [
      collection,
    ]);

    const start = new Date().getTime();
    const result = await replace(context, collection.name, value, {
      condition: { _id: { $exists: false } }, // condition to check user doesn't exists
    });
    console.log(`${showTimeTaken(start)} insert by replace`);
    expect(result).toHaveProperty('_id');
  });

  test('fetch items using transaction', async () => {
    const context = createContext(localDDBClient as unknown as DynamoDBClient, [
      collection,
    ]);

    const items: TransactFindByIdDescriptor[] = [
      {
        id: 'test-sh',
        collection: collection.name,
      },
      {
        id: 'test-id',
        collection: collection.name,
      },
    ];
    const start = new Date().getTime();
    const result = await transactFindByIds(context, items);
    console.log(`${showTimeTaken(start)} Fetched by Get Transaction`);

    expect(result).toEqual([
      { name: 'Sherlock', email: 'sh@sh.com', _id: 'test-sh' },
      { name: 'Moriarty', email: 'moriarty@jim.com', _id: 'test-id' },
    ]);
  });

  test('write a transaction to ddb consisting multiple ops', async () => {
    const context = createContext(localDDBClient as unknown as DynamoDBClient, [
      collection,
    ]);

    const request = [
      {
        collectionName: collection.name,
        value: {
          _id: 'test-jw',
          lastName: 'Watson',
          firstName: 'John',
          email: 'jw@sh.sh',
        },
        options: { condition: { _id: { $exists: false } } }, // an insertion
      },
      {
        collectionName: collection.name,
        value: {
          _id: 'test-sh',
          lastName: 'Holmes',
          firstName: 'Sherlock',
          email: 'sh@sh.sh',
        }, // an update to existing user
      },
      {
        collectionName: collection.name,
        id: 'test-id',
      }, // a deletion
    ] as TransactionWriteRequest[];

    transactionWrite(context, request);
  });

  test('fetch inserted, updated or deleted items using transaction', async () => {
    const context = createContext(localDDBClient as unknown as DynamoDBClient, [
      collection,
    ]);

    const items: TransactFindByIdDescriptor[] = [
      {
        id: 'test-sh',
        collection: collection.name,
      },
      {
        id: 'test-jw',
        collection: collection.name,
      },
      {
        id: 'test-id',
        collection: collection.name,
      },
    ];

    const start = new Date().getTime();
    const result = await transactFindByIds(context, items);
    console.log(`${showTimeTaken(start)} Fetch by Get Transaction`);

    expect(result).toEqual([
      {
        lastName: 'Holmes',
        firstName: 'Sherlock',
        _id: 'test-sh',
        email: 'sh@sh.sh',
      },
      {
        lastName: 'Watson',
        firstName: 'John',
        _id: 'test-jw',
        email: 'jw@sh.sh',
      },
    ]);
  });
});
