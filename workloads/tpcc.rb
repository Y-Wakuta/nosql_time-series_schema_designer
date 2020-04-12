# frozen_string_literal: true

NoSE::Workload.new do
  Model 'tpcc'

  # Define queries and their relative weights, weights taken from below
  DefaultMix :write_heavy

  Group 'NewOrderSelect', basic: 10,
                          write_mediam: 10,
                          write_heavy: 0.1 do
    Q 'SELECT customer.c_discount, customer.c_last, customer.c_credit, d_to_warehouse.w_tax FROM customer.c_to_district.d_to_warehouse WHERE d_to_warehouse.w_id = ? AND customer.c_d_id = ? AND customer.c_id = ? -- 0'
    Q 'SELECT district.d_next_o_id, district.d_tax FROM district WHERE district.d_id = ? AND district.d_w_id = ? -- 1'
   Q 'SELECT item.i_price, item.i_name, item.i_data FROM item WHERE item.i_id = ? -- 5'
    Q 'SELECT stock.s_quantity, stock.s_data, stock.s_dist_one, stock.s_dist_two, stock.s_dist_three, stock.s_dist_four, stock.s_dist_five, stock.s_dist_six, stock.s_dist_seven, stock.s_dist_eight, stock.s_dist_nine, stock.s_dist_ten FROM stock WHERE stock.s_i_id = ? AND stock.s_w_id = ? -- 6'
  end

  Group 'NewOrderUpdate', basic: 10,
                          write_mediam: 10 * 10,
                          write_heavy: 10 * 100 do
    Q 'UPDATE district SET d_next_o_id = ? WHERE district.d_id = ? AND district.d_w_id = ? -- 2'
    Q 'INSERT INTO orders SET o_id=?, o_d_id=?, o_w_id=?, o_c_id=?, o_entry_d=?, o_ol_cnt=?, o_all_local=? AND CONNECT TO o_to_customer(?) -- 3'
    Q 'INSERT INTO new_orders SET no_o_id = ?, no_d_id = ?, no_w_id = ? '\
      'AND CONNECT TO new_to_order(?) -- 4'
    Q 'UPDATE stock SET s_quantity = ? WHERE stock.s_i_id = ? AND stock.s_w_id = ? -- 7'
    Q 'INSERT INTO order_line SET ol_o_id = ?, ol_d_id = ?, ol_w_id = ?, ol_number = ?, ol_i_id = ?, ol_supply_w_id = ?, ol_quantity = ?, ol_amount = ?, ol_dist_info = ? AND CONNECT TO ol_to_order(?), ol_to_stock(?) -- 8'
  end

  Group 'PaymentSelect', basic: 10,
                         write_mediam: 10,
                         write_heavy: 0.1 do
    Q 'SELECT warehouse.w_street_one, warehouse.w_street_two, warehouse.w_city, warehouse.w_state, warehouse.w_zip, warehouse.w_name FROM warehouse WHERE warehouse.w_id = ? -- 10'
    Q 'SELECT district.d_street_one, district.d_street_two, district.d_city, district.d_state, district.d_zip, district.d_name FROM district WHERE district.d_w_id = ? AND district.d_id = ? -- 12'
    Q 'SELECT customer.c_id FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_last = ? -- 13'
    Q 'SELECT customer.c_id FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_last = ? ORDER BY customer.c_first -- 14'
    Q 'SELECT customer.c_first, customer.c_middle, customer.c_last, customer.c_street_one, customer.c_street_two, customer.c_city, customer.c_state, customer.c_zip, customer.c_phone, customer.c_credit, customer.c_credit_lim, customer.c_discount, customer.c_balance, customer.c_since ' \
      'FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_id = ? -- 15'
    Q 'SELECT customer.c_data FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_id = ? -- 16'
  end

  Group 'PaymentUpdate', basic: 10,
                         write_mediam: 10 * 10,
                         write_heavy: 10 * 100 do
    Q 'UPDATE warehouse SET w_ytd = ? WHERE warehouse.w_id = ? -- 10'
    Q 'UPDATE district SET d_ytd = ? WHERE district.d_w_id = ? AND district.d_id = ? -- 11'
    Q 'UPDATE customer SET c_balance = ?, c_data = ? WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_id = ? -- 17'
    Q 'UPDATE customer SET c_balance = ? WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_id = ? -- 18'
    Q 'INSERT INTO history SET h_c_d_id = ?, h_c_w_id = ?, h_c_id = ?, h_d_id = ?, h_w_id = ?, h_date = ?, h_amount = ?, h_data = ? AND CONNECT TO h_to_district(?), h_to_customer(?) -- 19'
  end

  Group 'OrderStat', basic: 1,
                     write_mediam: 1,
                     write_heavy: 0.1 do
    Q 'SELECT customer.c_id FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_last = ? -- 20'
    Q 'SELECT customer.c_balance, customer.c_first, customer.c_middle, customer.c_last FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_last = ? ORDER BY customer.c_first -- 21'
    Q 'SELECT customer.c_balance, customer.c_first, customer.c_middle, customer.c_last FROM customer WHERE customer.c_w_id = ? AND customer.c_d_id = ? AND customer.c_id = ? -- 22'
    Q 'SELECT orders.o_id, orders.o_entry_d, orders.o_carrier_id FROM orders WHERE orders.o_w_id = ? AND orders.o_d_id = ? AND orders.o_c_id = ? AND orders.o_id = ? -- 23'
    Q 'SELECT orders.o_id FROM orders WHERE orders.o_w_id = ? AND orders.o_d_id = ? AND orders.o_c_id = ? -- 23.5'
    Q 'SELECT order_line.ol_i_id, order_line.ol_supply_w_id, order_line.ol_quantity, order_line.ol_amount, order_line.ol_delivery_d FROM order_line WHERE order_line.ol_w_id = ? AND order_line.ol_d_id = ? AND order_line.ol_o_id = ? -- 24'
  end

  Group 'DeliverySelect', basic: 1,
                          write_mediam: 1,
                          write_heavy: 0.1 do
    Q 'SELECT new_orders.no_o_id FROM new_orders WHERE new_orders.no_d_id = ? AND new_orders.no_w_id = ? -- 25'
    Q 'SELECT orders.o_c_id FROM orders WHERE orders.o_id = ? AND orders.o_d_id = ? AND orders.o_w_id = ? -- 27'
    Q 'SELECT order_line.ol_amount FROM order_line WHERE order_line.ol_o_id = ? AND order_line.ol_d_id = ? AND order_line.ol_w_id = ? -- 30'
    Q 'SELECT district.d_next_o_id FROM district WHERE district.d_id = ? AND district.d_w_id = ? -- 32'
  end

  Group 'DeliveryUpdate', basic: 1,
                          write_mediam: 1 * 10,
                          write_heavy: 1 * 100 do
    Q 'DELETE new_orders FROM new_orders WHERE new_orders.no_o_id = ? AND new_orders.no_d_id = ? AND new_orders.no_w_id = ? -- 26'
    Q 'UPDATE orders SET o_carrier_id = ? WHERE orders.o_id = ? AND orders.o_d_id = ? AND orders.o_w_id = ? -- 28'
    Q 'UPDATE order_line SET ol_delivery_d = ? WHERE order_line.ol_o_id = ? AND order_line.ol_d_id = ? AND order_line.ol_w_id = ? -- 29'
    Q 'UPDATE customer SET c_balance = ? , c_delivery_cnt = ? WHERE customer.c_id = ? AND customer.c_d_id = ? AND customer.c_w_id = ? -- 31'
  end

  Group 'Slev', basic: 1,
                write_mediam: 1,
                write_heavy: 0.1 do
    Q 'SELECT district.d_next_o_id FROM district WHERE district.d_id = ? AND district.d_w_id = ? -- 33'
    Q 'SELECT order_line.ol_i_id FROM order_line WHERE order_line.ol_w_id = ? AND order_line.ol_d_id = ? AND order_line.ol_o_id < ? AND order_line.ol_o_id >= ? -- 34'
    Q 'SELECT stock.* FROM stock WHERE stock.s_w_id = ? AND stock.s_i_id = ? AND stock.s_quantity < ? -- 35'
  end
end
