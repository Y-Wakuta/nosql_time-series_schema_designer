# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'warehouse' do
    ID     'w_id'
    String 'w_name', 10
    String 'w_street_one', 20
    String 'w_street_two', 20
    String 'w_city', 20
    String 'w_state', 2
    String 'w_zip', 9
    Float  'w_tax'
    Float  'w_ytd'
  end) * 50

  (Entity 'district' do
    ID      'd_id'
    ID      'd_w_id', count: 1
    String  'd_name', 10
    String  'd_street_one', 20
    String  'd_street_two', 20
    String  'd_city', 20
    String  'd_state', 2
    String  'd_zip', 9
    Integer 'd_tax'
    Integer 'd_ytd'
    Integer 'd_next_o_id'
  end) * 5

  (Entity 'customer' do
    ID        'c_id'
    ID        'c_d_id'
    ID        'c_w_id'
    String    'c_first', 16
    String    'c_middle', 2
    String    'c_last', 16
    String    'c_street_one', 20
    String    'c_street_two', 20
    String    'c_city', 20
    String    'c_state', 2
    String    'c_zip', 9
    String    'c_phone', 16
    Date      'c_since'
    String    'c_credit', 2
    Integer   'c_credit_lim'
    Float     'c_discount'
    Float     'c_balance'
    Float     'c_ytd_payment'
    Integer   'c_payment_cnt'
    Integer   'c_delivery_cnt'
    String    'c_data'
  end) * 2_000

  (Entity 'history' do
    ID         'h_c_id'
    Integer    'h_c_d_id'
    Integer    'h_c_w_id'
    Integer    'h_d_id'
    Integer    'h_w_id'
    Float      'h_amount'
    String     'h_data',24
    Date       'h_date'
  end) * 20_000

  (Entity 'new_orders' do
    ID         'no_o_id'
    ID         'no_d_id'
    ID         'no_w_id'
  end) * 200_000

  (Entity 'orders' do
    ID         'o_id'
    ID         'o_d_id'
    ID         'o_w_id'
    Integer    'o_c_id'
    Date       'o_entry_d'
    Integer    'o_carrier_id'
    Integer    'o_ol_cnt'
    Integer    'o_all_local'
  end) * 100_000

  (Entity 'order_line' do
    ID         'ol_o_id'
    ID         'ol_d_id'
    ID         'ol_w_id'
    ID         'ol_number'
    Integer    'ol_i_id'
    Integer    'ol_supply_w_id'
    Date       'ol_delivery_d'
    Integer    'ol_quantity'
    Float      'ol_amount'
    String     'ol_dist_info',24
  end) * 40_000


  (Entity 'item' do
    ID         'i_id'
    Integer    'i_im_id'
    String     'i_name'
    Float      'i_price'
    String     'i_data',50
  end) * 200_000

  (Entity 'stock' do
    ID         's_i_id'
    ID         's_w_id'
    Integer    's_quantity'
    String     's_dist_one',24
    String     's_dist_two',24
    String     's_dist_three',24
    String     's_dist_four',24
    String     's_dist_five',24
    String     's_dist_six',24
    String     's_dist_seven',24
    String     's_dist_eight',24
    String     's_dist_nine',24
    String     's_dist_ten',24
    Float      's_ytd'
    Float      's_order_cnt'
    Float      's_remote_cnt'
    String     's_data',50
  end)

  HasOne 'new_to_order',       'orders',
         'new_orders'      => 'orders'

  HasOne 'ol_to_order',       'orders',
         'order_line'      => 'orders'

  HasOne 'ol_to_stock',     'stock',
         'order_line'      => 'stock'

  HasOne 's_to_warehouse',         'warehouse',
         'stock'       => 'warehouse'

  HasOne 's_to_item',         'item',
         'stock'       => 'item'

  HasOne 'o_to_customer',    'customer',
         'orders'   => 'customer'

  HasOne 'd_to_warehouse',      'warehouse',
         'district'   => 'warehouse'

  HasOne 'c_to_district',         'district',
         'customer'   => 'district'

  HasOne 'h_to_district',        'district',
         'history'     => 'district'

  HasOne 'h_to_customer',         'customer',
         'history'     => 'customer'
end
# rubocop:enable all
