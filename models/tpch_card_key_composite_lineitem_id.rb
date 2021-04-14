# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'part' do
    ID 'p_partkey', count: 200_000
    String 'p_name', count: 199_997
    String 'p_mfgr', count: 5
    String 'p_brand', count: 25
    String 'p_type', count: 150
    Integer 'p_size', count: 50
    String 'p_container', count: 40
    Float 'p_retailprice', count: 20_899
    Integer 'p_comment', count: 131_749
  end) * 2000_000

  (Entity 'supplier' do
    ID 's_suppkey', count: 10_000
    #Integer 's_nationkey'
    String 's_name', count: 10_000
    String 's_address', count: 10_000
    String 's_phone', count: 10_000
    Float 's_acctbal', count: 10_000
    String 's_comment', count: 10_000
  end) * 10_000

  (Entity 'partsupp' do
    ID 'ps_partkey', count: 200_000, composite: ['ps_suppkey']
    ID 'ps_suppkey', count: 10_000
    Integer 'ps_availqty', count: 9_999
    Float 'ps_supplycost', count: 99_865
    String 'ps_comment', count: 799_124
  end) * 800_000

  (Entity 'customer'do
    ID 'c_custkey', count: 150_000
    String 'c_name', count: 150_000
    String 'c_address', count: 150_000
    String 'c_phone', count: 150_000
    Float 'c_acctbal', count: 140_187
    #Integer 'c_nationkey'
    String 'c_mktsegment', count: 5
    String 'c_comment', count: 149968
  end) * 150_000

  (Entity 'orders' do
    ID 'o_orderkey', count: 1_500_000
    #Integer 'o_custkey'
    String 'o_orderstatus', count: 3
    Float 'o_totalprice', count: 1464556
    Date 'o_orderdate', count: 2_406
    String 'o_orderpriority', count: 5
    String 'o_clerk', count: 1_000
    Integer 'o_shippriority', count: 1
    String 'o_comment', count: 1_482_071
    Integer 'dummy', count: 1
  end) * 1_500_000

  (Entity 'lineitem' do
    ID 'l_orderkey', composite: ['l_linenumber']
    CompositeKey 'l_linenumber', count: 6_000_000
    #Integer 'l_suppkey'
    #Integer 'l_partkey'
    Float 'l_quantity', count: 50
    Float 'l_extendedprice', count: 933_900
    Float 'l_discount', count: 11
    Float 'l_tax', count: 9
    String 'l_returnflag', count: 3
    String 'l_linestatus', count: 2
    Date 'l_shipdate', count: 2_526
    Date 'l_commitdate', count: 2_466
    Date 'l_receiptdate', count: 2_554
    String 'l_shipmode', count: 7
    String 'l_shipinstruct', count: 4
    String 'l_comment', count: 4_580_554
    Integer 'dummy', count: 1
  end) * 6_000_000

  (Entity 'nation' do
    ID 'n_nationkey', count: 25
    #Integer 'n_regionkey'
    String 'n_name', count: 25
    String 'n_comment', count: 25
  end) * 25

  (Entity 'region' do
    ID 'r_regionkey', count: 5
    String 'r_name', count: 5
    String 'r_comment', count: 5
  end) * 5

  HasOne 's_nationkey',       'from_supplier',
         'supplier'      => 'nation'

  HasOne 'c_nationkey',       'from_customer',
         'customer'      => 'nation'

  HasOne 'ps_suppkey', 'from_partsupp',
         'partsupp' => 'supplier'

  HasOne 'n_regionkey', 'from_nation',
         'nation'  =>  'region'

  HasOne 'o_custkey',       'from_orders',
         'orders'      => 'customer'

  HasOne 'l_orderkey',       'from_lineitem',
         'lineitem'      => 'orders'

  HasOne 'l_partkey', 'from_lineitem',
         {'lineitem' => 'partsupp'}, composite: [{"name" => "l_suppkey", "related_key" => "ps_suppkey"}]

  # TODO: currently entities cannot have multiple foreign keys
  HasOne 'l_suppkey', 'from_lineitem_supp',
  'lineitem' => 'partsupp'

  HasOne 'ps_partkey', 'from_partsupp',
         'partsupp' => 'part'

end
# rubocop:enable all
