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
    Integer 'p_retailprice'
    Integer 'p_comment'
  end) * 2000_000

  (Entity 'supplier' do
    ID 's_suppkey'
    String 's_name'
    String 's_address'
    String 's_phone'
    Integer 's_acctbal'
    String 's_comment'
  end) * 10_000

  (Entity 'partsupp' do
    ID 'ps_partkey'
    ID 'ps_suppkey'
    Integer 'ps_availqty'
    Integer 'ps_supplycost'
    String 'ps_comment'
  end) * 800_000

  (Entity 'customer'do
    ID 'c_custkey'
    String 'c_name'
    String 'c_address'
    String 'c_phone'
    Integer 'c_acctbal'
    String 'c_mktsegment'
    String 'c_comment'
  end) * 150_000

  (Entity 'orders' do
    ID 'o_orderkey'
    String 'o_orderstatus'
    Integer 'o_totalprice'
    Date 'o_orderdate'
    String 'o_orderpriority'
    String 'o_clerk'
    Integer 'o_shippriority'
    String 'o_comment'
    Integer 'dummy', count: 1
  end) * 1_500_000

  (Entity 'lineitem' do
    ID 'l_orderkey'
    Integer 'l_linenumber'
    Integer 'l_quantity'
    Integer 'l_extendedprice'
    Integer 'l_discount'
    Integer 'l_suppkey'
    Integer 'l_tax'
    String 'l_returnflag'
    Integer 'l_linestatus'
    Date 'l_shipdate'
    Date 'l_commitdate'
    Date 'l_receiptdate'
    String 'l_shipmode'
    String 'l_shipinstruct'
    String 'l_comment'
    Integer 'dummy', count: 1
  end) * 6_000_000

  (Entity 'nation' do
    ID 'n_nationkey'
    String 'n_name'
    String 'n_comment'
  end) * 25

  (Entity 'region' do
    ID 'r_regionkey'
    String 'r_name'
    String 'r_comment'
  end) * 5

  HasOne 'to_nation',       'from_supplier',
         'supplier'      => 'nation'

  HasOne 'to_nation',       'from_customer',
         'customer'      => 'nation'

  HasOne 'to_supplier',       'from_customer',
         'customer'      => 'supplier'

  HasOne 'to_supplier', 'from_partsupp',
         'partsupp' => 'supplier'

  HasOne 'to_region', 'from_nation',
         'nation'  =>  'region'

  HasOne 'to_customer',       'from_orders',
         'orders'      => 'customer'

  HasOne 'to_orders',       'from_lineitem',
         'lineitem'      => 'orders'

  HasOne 'to_partsupp', 'from_lineitem',
         'lineitem' => 'partsupp'

  HasOne 'to_part', 'from_partsupp',
         'partsupp' => 'part'

end
# rubocop:enable all
