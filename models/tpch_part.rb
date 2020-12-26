# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'part' do
    ID 'p_partkey'
    String 'p_name'
    String 'p_mfgr'
    String 'p_brand'
    String 'p_type'
    Integer 'p_size'
    String 'p_container'
    Float 'p_retailprice'
    Integer 'p_comment'
  end) * 2000_000

  (Entity 'supplier' do
    ID 's_suppkey'
    #Integer 's_nationkey'
    String 's_name'
    String 's_address'
    String 's_phone'
    Float 's_acctbal'
    String 's_comment'
  end) * 10_000

  (Entity 'partsupp' do
    #ID 'ps_partkey'
    ID 'ps_suppkey'
    Integer 'ps_availqty'
    Float 'ps_supplycost'
    String 'ps_comment'
  end) * 800_000

  (Entity 'lineitem' do
    #Integer 'l_orderkey'
    #Integer 'l_suppkey'
    #Integer 'l_partkey'
    ID 'l_linenumber'
    Float 'l_quantity'
    Float 'l_extendedprice'
    Float 'l_discount'
    Float 'l_tax'
    String 'l_returnflag'
    String 'l_linestatus'
    Date 'l_shipdate'
    Date 'l_commitdate'
    Date 'l_receiptdate'
    String 'l_shipmode'
    String 'l_shipinstruct'
    String 'l_comment'
    Integer 'dummy', count: 1
  end) * 6_000_000

  HasOne 'ps_suppkey', 'from_partsupp',
         'partsupp' => 'supplier'

  HasOne 'l_partkey', 'from_lineitem',
         'lineitem' => 'partsupp'

  HasOne 'l_suppkey', 'from_lineitem',
         'lineitem' => 'partsupp'

  HasOne 'ps_partkey', 'from_partsupp',
         'partsupp' => 'part'

end
# rubocop:enable all
