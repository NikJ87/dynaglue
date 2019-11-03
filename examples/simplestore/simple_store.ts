import { DynamoDB } from 'aws-sdk';
import { Collection, createContext, insert, listAll, findById, find, deleteById } from '../../lib';

const DYNAMODB_ENDPOINT = process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000';

/**
 *  in order to use this layout, you will need to start up DynamoDB local
 *  and provision the table
 *
 *  docker run -p 8000:8000 amazon/dynamodb-local
 *
 *  aws dynamodb create-table \
 *    --endpoint-url http://localhost:8000 \
 *    --table-name global \
 *    --attribute-definitions AttributeName=id,AttributeType=S AttributeName=collection,AttributeType=S \
 *      AttributeName=gs2p,AttributeType=S AttributeName=gs2s,AttributeType=S \
 *    --key-schema KeyType=HASH,AttributeName=id KeyType=SORT,AttributeName=collection \
 *    --billing-mode PAY_PER_REQUEST \
 *    --global-secondary-indexes 'IndexName=gs1,KeySchema=[{KeyType="HASH",AttributeName=collection},{KeyType=SORT,AttributeName=id}],Projection={ProjectionType=ALL}' \
 *      'IndexName=gs2,KeySchema=[{KeyType="HASH",AttributeName="gs2p"},{KeyType=SORT,AttributeName=gs2s}],Projection={ProjectionType=ALL}'
 */

const globalTableLayout = {
  tableName: 'global',
  primaryKey: {
    partitionKey: 'id',
    sortKey: 'collection',
  },
  listAllKey: {
    indexName: 'gs1',
    partitionKey: 'collection',
    sortKey: 'id',
  },
  findKeys: [
    {
      indexName: 'gs2',
      partitionKey: 'gs2p',
      sortKey: 'gs2s',
    },
  ],
};

const usersCollection: Collection = {
  name: 'users',
  layout: globalTableLayout,
  accessPatterns: [
    { indexName: 'gs2', partitionKeys: [], sortKeys: [['email']] }
  ]
};

const postsCollection: Collection = {
  name: 'posts',
  layout: globalTableLayout,
  accessPatterns: [
    { indexName: 'gs2', partitionKeys: [['userId']], sortKeys: [] },
  ]
}

async function main() {
  const ddb = new DynamoDB({ endpoint: DYNAMODB_ENDPOINT, region: 'us-east-1' });
  const ctx = createContext(ddb, [usersCollection, postsCollection]);

  console.log(`Connecting at endpoint ${ddb.endpoint.href}`);
  const tables = await ddb.listTables().promise();
  console.log('tables: ', tables.TableNames);

  const user1 = await insert(ctx, 'users', {
    name: 'Anayah Dyer',
    email: 'anayahd@example.com',
  });
  const user2 = await insert(ctx, 'users', {
    name: 'Ruairidh Hughes',
    email: 'ruairidhh@example.com',
  });
  const user3 = await insert(ctx, 'users', {
    name: 'Giles Major',
    email: 'gilesm@example.com',
  });

  console.log('inserted users', [user1, user2, user3]);

  const post1 = await insert(ctx, 'posts', {
    userId: user1._id,
    title: 'How to cook an apple pie'
  });

  const post2 = await insert(ctx, 'posts', {
    userId: user1._id,
    title: 'Cooking for a dinner party'
  });

  const post3 = await insert(ctx, 'posts', {
    userId: user2._id,
    title: 'My first blog post',
  });

  console.log('inserted posts', [post1, post2, post3]);

  const { items: allUsers } = await listAll(ctx, 'users');
  console.log('all users', allUsers);

  const { items: allPosts } = await listAll(ctx, 'posts');
  console.log('all posts', allPosts);

  const foundUser2 = await findById(ctx, 'users', user2._id);
  const notFoundUser4 = await findById(ctx, 'users', 'not-found-id');

  console.log('user 2', foundUser2);
  console.log('non-existent user 4', notFoundUser4);

  const postsByUser1 = await find(ctx, 'posts', { userId: user1._id });
  console.log('posts by user 1', postsByUser1.items);

  const deletedItem = await deleteById(ctx, 'posts', post2._id);
  console.log('deleted post #2', deletedItem);

  const emailSearch = await find(ctx, 'users', { email: 'anayah' });
  console.log('email search results', emailSearch);
}

main();
