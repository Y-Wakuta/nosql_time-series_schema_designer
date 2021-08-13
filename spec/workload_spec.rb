module NoSE
  describe Workload do
    subject(:workload) { Workload.new }
    let(:entity)      { Entity.new('Foo') << field }
    let(:field)       { Fields::IDField.new('Id') }

    before(:each) do
      workload.model.add_entity entity
    end

    context 'when adding items' do
      it 'holds entities' do
        expect(workload.model.entities).to have(1).item
        expect(workload.model['Foo']).to be entity
      end

      it 'automatically parses queries' do
        valid_query = Statement.parse 'SELECT Foo.Id FROM Foo ' \
                                      'WHERE Foo.Id = ?', workload.model
        workload.add_statement valid_query

        expect(workload.queries).to have(1).item
        expect(workload.queries.first).to be_a Query
      end

      it 'only accepts entities and queries' do
        expect { workload << 3 }.to raise_error TypeError
      end
    end

    it 'can find statements with a given tag' do
      query = Statement.parse 'SELECT Foo.Id FROM Foo WHERE Foo.Id = ? -- foo',
                              workload.model
      workload.add_statement query

      expect(workload.find_with_tag 'foo').to eq(query)
    end

    it 'can find fields on entities from queries' do
      expect(workload.model.find_field %w(Foo Id)).to be field
    end

    it 'can find fields which traverse foreign keys' do
      other_entity = Entity.new 'Bar'
      other_field = Fields::IDField.new 'Quux'
      other_entity << other_field
      workload.model.add_entity other_entity

      entity << Fields::ForeignKeyField.new('Baz', other_entity)

      expect(workload.model.find_field %w(Foo Baz Quux)).to be other_field
    end

    it 'raises an exception for nonexistent entities' do
      expect { workload.model['Bar'] }.to raise_error EntityNotFound
    end

    it 'can produce an image of itself' do
      expect_any_instance_of(GraphViz).to \
        receive(:output).with(png: '/tmp/rubis.png')
      workload.model.output :png, '/tmp/rubis.png'
    end

    it 'can remove updates' do
      entity << Fields::IntegerField.new('Bar')

      valid_query = Statement.parse 'SELECT Foo.Id FROM Foo WHERE Foo.Id = ?',
                                    workload.model
      workload.add_statement valid_query
      update = Statement.parse 'UPDATE Foo SET Bar = ? WHERE Foo.Id = ?',
                               workload.model
      workload.add_statement update

      workload.remove_updates
      expect(workload.queries).not_to be_empty
      expect(workload.updates).to be_empty
    end

    it 'can group statements' do
      query1 = 'SELECT Foo.Bar FROM Foo WHERE Foo.Id = ?'
      query2 = 'SELECT Foo.Baz FROM Foo WHERE Foo.Id = ?'

      workload = Workload.new do
        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
          String 'Baz'
        end

        Group 'Test1', 0.5 do
          Q query1
        end

        Group 'Test2', 0.5 do
          Q query2
        end
      end

      expect(workload.statement_weights).to eq(
                                              Statement.parse(query1, workload.model) => 0.5,
                                              Statement.parse(query2, workload.model) => 0.5
                                            )
    end
  end

  describe TimeDependWorkload do
    let(:time_steps)       { 3 }
    let(:query) {'SELECT Foo.Bar FROM Foo WHERE Foo.Id = ?'}
    let(:freq_array){[1.2,1.3,1.4]}
    let(:td_workload_float_array) {
      q = query
      fa = freq_array
      ts = time_steps
      TimeDependWorkload.new do
        TimeSteps ts
        DefaultMix :default
        Interval 60

        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
        end

        Group 'Test1', 0.5, default: fa do
          Q q
        end
      end
    }

    context "add frequency type for the statement"  do
      it "specify the frequency array" do
        weights = td_workload_float_array
                    .statement_weights
                    .select{|q, _| q.text == query}
                    .map{|_, weights| weights}
        expect(weights.first.size).to eq freq_array.size
      end
    end

    it 'allows to re-set interval' do
      q = query
      fa = freq_array
      ts = time_steps
      td_workload_200interval = TimeDependWorkload.new do
        TimeSteps ts
        DefaultMix :default
        Interval 200

        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
        end

        Group 'Test1', 0.5, default: fa do
          Q q
        end
      end

      td_workload_float_array.reset_interval 200
      expect(td_workload_float_array.statement_weights).to eq(td_workload_200interval.statement_weights)
      expect(td_workload_float_array.interval).to eq(td_workload_200interval.interval)
    end

    let(:td_workload_workload_ratio) {
      q = query
      TimeDependWorkload.new do
        TimeSteps 4
        DefinitionType DEFINITION_TYPE::WORKLOAD_SET_RATIO
        StartWorkloadSet :type1, 0.9
        EndWorkloadSet :type2, 0.1

        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
        end

        Group 'Test1', type1: 0.1, type2: 1 do
          Q q
        end
      end
    }

    it 'properly mixes the two type of workload frequency' do
      weights = td_workload_workload_ratio
                  .statement_weights
                  .select{|q, _| q.text == query}
                  .map{|_, weights| weights}
                  .first

      expected_first_freq = (0.1 * 0.9 + 1 * 0.1) * td_workload_workload_ratio.interval
      expected_last_freq = (0.1 * 0.1 + 1 * 0.9) * td_workload_workload_ratio.interval
      expect((weights.first - expected_first_freq).abs).to be < 0.01
      expect((weights.last - expected_last_freq).abs).to be < 0.01
    end

    it 'changes frequency type afterwards as static' do
      q = query
      fa = freq_array
      ts = time_steps
      workload_static = TimeDependWorkload.new do
        TimeSteps ts
        DefaultMix :default
        Interval 60
        Static true

        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
        end

        Group 'Test1', 0.5, default: fa do
          Q q
        end
      end
      td_workload_float_array.set_frequency_type("static")
      expect(workload_static).to eq(td_workload_float_array)
    end

    it 'changes frequency type afterwards as first' do
      q = query
      fa = freq_array
      ts = time_steps
      workload_first = TimeDependWorkload.new do
        TimeSteps ts
        DefaultMix :default
        Interval 60
        FirstTs true

        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
        end

        Group 'Test1', 0.5, default: fa do
          Q q
        end
      end
      td_workload_float_array.set_frequency_type("firstTs")
      expect(workload_first).to eq(td_workload_float_array)
    end

    it 'changes frequency type afterwards as last' do
      q = query
      fa = freq_array
      ts = time_steps
      workload_last = TimeDependWorkload.new do
        TimeSteps ts
        DefaultMix :default
        Interval 60
        LastTs true

        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
        end

        Group 'Test1', 0.5, default: fa do
          Q q
        end
      end
      td_workload_float_array.set_frequency_type("lastTs")
      expect(workload_last).to eq(td_workload_float_array)
    end
  end
end
