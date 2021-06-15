module NoSE
  module Migrator
    describe Migrator do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'migrator collects data using materialized view plan' do
        target_index = Index.new [tweet['Body']], [tweet['TweetId']],
                                 [tweet['Timestamp']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 )
        index = Index.new [tweet['Body']], [tweet['TweetId']],
                          [tweet['Retweets'], tweet['Timestamp']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field]
                          )

        migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(index)
        planner = Plans::MigrateSupportSimpleQueryPlanner.new @workload, [index], cost_model, 2
        migrate_plan = planner.min_plan(migrate_support_query)
        expect(migrate_plan.indexes.size).to be 1
        expect(migrate_plan.indexes.first).to be index

        index_values = Hash[index, {
          "Tweet_Body" => "body", "Tweet_TweetId" => "0",
          "Tweet_Retweets" => "100", "Tweet_Timestamp" => "12213"
        }]
        related_index_values = Hash[index, target_index.all_fields.map{|f| index_values[index].slice(f.id)}.reduce(&:merge)]
        migrator = Migrator.new nil, nil, nil, nil, false
        join_result = migrator.send(:full_outer_join, related_index_values)
        expect(join_result).to be related_index_values[index]
      end

      it 'migrator collects data using join view plan' do
        workload_comp = workload_composite_key
        user_comp = workload_comp.model["User"]
        tweet_comp = workload_comp.model["Tweets"]

        target_index = Index.new [user_comp['Username'], user_comp['UserId']], [tweet_comp['TweetId'], tweet_comp['FollowerId']],
                                 [tweet_comp['Body'], tweet_comp['Timestamp']], QueryGraph::Graph.from_path([user_comp.id_field, user_comp['Tweets']])

        composite_parent_index = Index.new [user_comp['Username']],
                                           [user_comp['UserId'], tweet_comp['TweetId'], tweet_comp['FollowerId']], [],
                                           QueryGraph::Graph.from_path([user_comp.id_field, user_comp['Tweets']])
        composite_last_index = Index.new [tweet_comp['TweetId']] , [tweet_comp['FollowerId']],
                                         [tweet_comp['Body'], tweet_comp['Timestamp']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        planner = Plans::MigrateSupportSimpleQueryPlanner.new workload_comp, [composite_parent_index, composite_last_index], cost_model, 2
        migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(target_index)
        migrate_plan = planner.min_plan(migrate_support_query)
        expect(migrate_plan.indexes.size).to be 2
        expect(migrate_plan.indexes.first).to be composite_parent_index
        expect(migrate_plan.indexes.last).to be composite_last_index

        index_values = {composite_parent_index =>
                          [{
                             "User_Username" => "Bob", "User_UserId" => "10",
                             "Tweets_TweetId" => "0", "Tweets_FollowerId" => "100"
                           },
                           {
                             "User_Username" => "Bob", "User_UserId" => "10",
                             "Tweets_TweetId" => "0", "Tweets_FollowerId" => "101"
                           },
                           {
                             "User_Username" => "Bob", "User_UserId" => "0",
                             "Tweets_TweetId" => "1", "Tweets_FollowerId" => "200"
                           }],
                        composite_last_index =>
                          [{
                             "Tweets_TweetId" => "0", "Tweets_FollowerId" => "100",
                             "Tweets_Body" => "body", "Tweets_Timestamp" => "12213"
                           },
                          {
                             "Tweets_TweetId" => "2", "Tweets_FollowerId" => "100",
                             "Tweets_Body" => "body", "Tweets_Timestamp" => "12213"
                           }],
        }
        migrator = Migrator.new nil, nil, nil, nil, false

        expected_join_result = [
          {
            "User_Username" => "Bob", "User_UserId" => "10",
            "Tweets_TweetId" => "0", "Tweets_FollowerId" => "100",
            "Tweets_Body" => "body", "Tweets_Timestamp" => "12213"
          },
          migrator.send(:join_with_empty_record, {
            "User_Username" => "Bob", "User_UserId" => "10",
            "Tweets_TweetId" => "0", "Tweets_FollowerId" => "101"}, composite_last_index),
          migrator.send(:join_with_empty_record, {
            "User_Username" => "Bob", "User_UserId" => "0",
            "Tweets_TweetId" => "1", "Tweets_FollowerId" => "200" }, composite_last_index),
          migrator.send(:join_with_empty_record, {
                             "Tweets_TweetId" => "2", "Tweets_FollowerId" => "100",
                             "Tweets_Body" => "body", "Tweets_Timestamp" => "12213" }, composite_parent_index)
        ]

        actual_join_result = migrator.send(:full_outer_join, index_values)
        expect(actual_join_result.to_set).to eq(expected_join_result.to_set)
      end
    end
  end
end


