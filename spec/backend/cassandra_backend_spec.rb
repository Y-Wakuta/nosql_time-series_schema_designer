require 'nose/backend/cassandra'

module NoSE
  module Backend
    shared_context 'dummy Cassandra backend' do
      include_context 'dummy cost model'
      include_context 'entities'

      let(:backend) { CassandraBackend.new workload, [index], [], [], {} }
    end

    describe CassandraBackend do
      include_examples 'backend processing', cassandra: true do
        let(:config) do
          {
            name: 'cassandra',
            hosts: ['127.0.0.1'],
            port: 9042,
            keyspace: 'nose'
          }
        end

        let(:backend) do
          CassandraBackend.new plans.schema.model, plans.schema.indexes.values,
                               [], [], config
        end

        before(:all) do
          next if RSpec.configuration.exclusion_filter[:cassandra]
          cluster = Cassandra.cluster hosts: ['127.0.0.1'], port: 9042,
                                      timeout: nil

          keyspace_definition = <<-KEYSPACE_CQL
            CREATE KEYSPACE "nose"
            WITH replication = {
              'class': 'SimpleStrategy',
              'replication_factor': 1
            }
          KEYSPACE_CQL

          session = cluster.connect

          keyspace = cluster.has_keyspace? 'nose'
          session.execute 'DROP KEYSPACE "nose"' if keyspace

          session.execute keyspace_definition
        end
      end

      it 'is a type of backend' do
        expect(CassandraBackend.subtype_name).to eq 'cassandra'
      end
    end

    describe CassandraBackend do
      context 'when not connected' do
        include_context 'dummy Cassandra backend'

        it 'can generate DDL for a simple index' do
          expect(backend.indexes_ddl).to match_array [
            'CREATE COLUMNFAMILY "TweetIndex" ("User_Username" text, ' \
            '"Tweet_Timestamp" date, "User_UserId" uuid, '\
            '"Tweet_TweetId" uuid, ' \
            '"Tweet_Body" text, ' \
            '"value_hash" text, PRIMARY KEY(("User_Username"), ' \
            '"Tweet_Timestamp", "User_UserId", "Tweet_TweetId", value_hash));'
          ]
        end
      end
    end

    describe CassandraBackend::IndexLookupStatementStep do
      include_context 'dummy Cassandra backend'

      it 'can lookup data for an index based on a plan' do
        # Materialize a view for the given query
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = "Bob" ' \
                                'ORDER BY Tweet.Timestamp LIMIT 10',
                                workload.model
        index = query.materialize_view
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        step = planner.min_plan(query).first

        # Validate the expected CQL query
        client = double('client')
        backend_query = 'SELECT "User_Username", "Tweet_Timestamp", ' \
                        '"Tweet_Body" ' + "FROM \"#{index.key}\" " \
                        'WHERE "User_Username" = ? ' \
                        'ORDER BY "Tweet_Timestamp" LIMIT 10'
        expect(client).to receive(:prepare).with(backend_query) \
          .and_return(backend_query)

        # Define a simple array providing empty results
        results = []
        def results.last_page?
          true
        end
        expect(client).to receive(:execute) \
          .with(backend_query, arguments: ['Bob']).and_return(results)

        step_class = CassandraBackend::IndexLookupStatementStep
        prepared = step_class.new client, query.all_fields, query.conditions,
                                  step, nil, step.parent
        prepared.process query.conditions, nil, nil
      end

      it 'produce query with composite key for each step' do
        workload_comp = workload_composite_key
        user_comp = workload_comp.model["User"]
        tweet_comp = workload_comp.model["Tweets"]
        model = workload_comp.model
        query =  Statement.parse 'SELECT Tweets.Body, Tweets.Timestamp FROM Tweets.User ' \
                      'WHERE User.Username = "Bob" AND Tweets.TweetId = 1 LIMIT 5', model
        workload_comp.add_statement query

        composite_parent_index = Index.new [user_comp['Username']],
                                         [user_comp['UserId'], tweet_comp['TweetId'], tweet_comp['FollowerId']], [],
                                         QueryGraph::Graph.from_path([user_comp.id_field, user_comp['Tweets']])
        composite_last_index = Index.new [tweet_comp['TweetId']] , [tweet_comp['FollowerId']],
                                         [tweet_comp['Body'], tweet_comp['Timestamp']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        planner = Plans::QueryPlanner.new workload_comp.model, [composite_parent_index, composite_last_index], cost_model
        plan = planner.min_plan(query)

        client = double('client')
        step_class = CassandraBackend::IndexLookupStatementStep

        #===============================================================
        # check first step
        #===============================================================
        first_step = plan.first
        backend_query = 'SELECT "User_Username", "Tweets_TweetId", "Tweets_FollowerId" ' \
                         + "FROM \"#{composite_parent_index.key}\" " \
                        'WHERE "User_Username" = ? LIMIT 5'
        expect(client).to receive(:prepare).with(backend_query) \
          .and_return(backend_query)
        prepared = step_class.new client, query.all_fields, query.conditions,
                                  first_step, nil, first_step.parent, [plan.last]
        results = [
            {"User_Username" => "Bob", "Tweets_TweetId" => "1", "Tweets_FollowerId" => "100"},
            {"User_Username" => "Bob", "Tweets_TweetId" => "2", "Tweets_FollowerId" => "101"}
        ]
        allow(CassandraBackend).to receive(:remove_any_null_place_holder_row).and_return(results)

        def results.last_page?
          true
        end
        expect(client).to receive(:execute) \
          .with(backend_query, arguments: ['Bob']).and_return(results)
        res = prepared.process query.conditions, nil, nil

        #===============================================================
        # check last step
        #===============================================================
        last_step = plan.last
        backend_query = 'SELECT "Tweets_TweetId", "Tweets_Body", "Tweets_Timestamp" ' \
                         + "FROM \"#{composite_last_index.key}\" " \
                        'WHERE "Tweets_TweetId" = ? AND "Tweets_FollowerId" = ? LIMIT 5'
        expect(client).to receive(:prepare).with(backend_query) \
          .and_return(backend_query)
        prepared = step_class.new client, query.all_fields, query.conditions,
                                  last_step, nil, last_step.parent
        expect(client).to receive(:execute) \
          .with(backend_query, arguments: [kind_of(Cassandra::Uuid), kind_of(Cassandra::Uuid)]) \
          .and_return(results).exactly(results.size).times
        prepared.process query.conditions, res, nil
      end

      it 'generates query for aggregation on database' do
        query = Statement.parse 'SELECT sum(Tweet.Body), max(Tweet.Retweets), User.Username FROM Tweet.User ' \
                                'WHERE User.Username = "Bob" ' \
                                'GROUP BY User.Username',
                                workload.model
        index = query.materialize_view_with_aggregation
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        step = planner.min_plan(query).last

        client = double('client')
        backend_query = 'SELECT "User_Username", sum("Tweet_Body"), max("Tweet_Retweets") ' \
                        + "FROM \"#{index.key}\" " \
                        'WHERE "User_Username" = ? GROUP BY "User_Username"'

        step_class = CassandraBackend::IndexLookupStatementStep
        expect(client).to receive(:prepare).with(backend_query) \
          .and_return(backend_query)
        step_class.new client, query.all_fields, query.conditions,
                                  step, nil, step.parent
      end
    end

    describe CassandraBackend::InsertStatementStep do
      include_context 'dummy Cassandra backend'

      it 'can insert into an index' do
        client = double('client')
        index = link.simple_index
        values = [{
          'Link_LinkId' => nil,
          'Link_URL' => 'http://www.example.com/'
        }]
        values = backend.send(:add_value_hash, index, values)
        backend_insert = "INSERT INTO #{index.key} (\"Link_LinkId\", " \
                         '"Link_URL", value_hash ) VALUES (?, ?, ?)'
        expect(client).to receive(:prepare).with(backend_insert) \
          .and_return(backend_insert)
        expect(client).to receive(:execute) \
          .with(backend_insert, arguments: [kind_of(Cassandra::Uuid),
                                            'http://www.example.com/', values.first['value_hash'].to_s])

        step_class = CassandraBackend::InsertStatementStep
        prepared = step_class.new client, index, [link['LinkId'], link['URL']]
        prepared.process values
      end
    end

    describe CassandraBackend::DeleteStatementStep do
      include_context 'dummy Cassandra backend'

      it 'can delete from an index' do
        client = double('client')
        index = link.simple_index
        backend_delete = "DELETE FROM #{index.key} WHERE \"Link_LinkId\" = ?"
        expect(client).to receive(:prepare).with(backend_delete) \
          .and_return(backend_delete)
        expect(client).to receive(:execute) \
          .with(backend_delete, arguments: [kind_of(Cassandra::Uuid)])

        step_class = CassandraBackend::DeleteStatementStep
        prepared = step_class.new client, index
        prepared.process [links.first['Link_LinkId']]
      end
    end

    describe CassandraBackend::AggregationStatementStep do
      include_context 'dummy Cassandra backend'

      it 'aggregates result rows' do
        # Materialize a view for the given query
        query = Statement.parse 'SELECT sum(Tweet.Body), max(Tweet.Retweets), User.Username FROM Tweet.User ' \
                                'WHERE User.Username = "Bob" ' \
                                'GROUP BY User.Username',
                                workload.model
        index = query.materialize_view
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        step = planner.min_plan(query).last

        client = double('client')

        results = [
            {'Tweet_Body' => '1', 'Tweet_Retweets' => '11', 'User_Username' => 'Bob'},
            {'Tweet_Body' => '2', 'Tweet_Retweets' => '12', 'User_Username' => 'Bob'},
            {'Tweet_Body' => '3', 'Tweet_Retweets' => '13', 'User_Username' => 'Alice'},
            {'Tweet_Body' => '4', 'Tweet_Retweets' => '14', 'User_Username' => 'Alice'},
        ]
        def results.last_page?
          true
        end
        step_class = CassandraBackend::AggregationStatementStep
        prepared = step_class.new client, query.all_fields, query.conditions,
                                  step, nil, step.parent
        actual = prepared.process query.conditions, results, nil
        expected = [
          {'Tweet_Body' => 3.0, 'Tweet_Retweets' => 12.0, 'User_Username' => "Bob"},
          {'Tweet_Body' => 7.0, 'Tweet_Retweets' => 14.0, 'User_Username' => "Alice"}
        ]
        expect(actual).to eq(expected)
      end
    end
  end
end
