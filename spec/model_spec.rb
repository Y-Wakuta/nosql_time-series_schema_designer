module NoSE
  describe Entity do
    subject(:entity) { Entity.new('Foo') }

    it 'can store fields' do
      entity << Fields::IntegerField.new('Bar')
      entity << Fields::IntegerField.new('Baz')

      expect(entity.fields.keys).to match_array %w(Bar Baz)
    end

    it 'can have foreign keys' do
      other = entity * 100
      field = Fields::ToOneKeyField.new('other', other)
      entity << field

      expect(field.entity).to be(other)
      expect(field.class.subtype_name).to eq('foreign_key')
      expect(field.relationship).to eq(:one)
      expect(field.cardinality).to eq(100)
    end

    it 'can have foreign keys with cardinality > 1' do
      others = entity * 100
      field = Fields::ToManyKeyField.new('others', others)
      entity << field

      expect(field.entity).to be(others)
      expect(field.class.subtype_name).to eq('to_many')
      expect(field.relationship).to eq(:many)
      expect(field.cardinality).to eq(100)
    end

    it 'can tell fields when they are added' do
      field = Fields::IntegerField.new('Bar')

      expect(field.parent).to be_nil

      entity << field

      expect(field.parent).to be(entity)
    end

    it 'can create entities using a DSL' do
      entity = Entity.new 'Foo' do
        ID 'Bar'
        Integer 'Baz'
        String 'Quux', 20
        etc
      end

      expect(entity.fields).to have(4).items
      expect(entity.fields['Quux'].size).to eq 20
      expect(entity['**'].class).to be Fields::HashField
    end

    it 'raises an exception for nonexistent fields' do
      expect { entity['Bar'] }.to raise_error FieldNotFound
    end
  end

  describe Fields::Field do
    subject(:field) do
      Fields::IDField.new 'Bar'
    end

    it 'has an ID based on the entity and name' do
      Entity.new('Foo') << field
      expect(field.id).to eq 'Foo_Bar'
    end

    it 'can have its cardinality updated by multiplication' do
      expect((field * 5).cardinality).to eq 5
    end

    it 'is never a foreign key' do
      expect(field.foreign_key_to? Entity.new('Baz')).to be_falsy
    end
  end

  describe Fields::IntegerField do
    it 'can convert string literals' do
      expect(Fields::IntegerField.value_from_string '42').to eq 42
    end
  end

  describe Fields::FloatField do
    it 'can convert string literals' do
      expect(Fields::FloatField.value_from_string '3.14159').to eq 3.14159
    end
  end

  describe Fields::StringField do
    it 'can convert string literals' do
      expect(Fields::StringField.value_from_string 'pudding').to eq 'pudding'
    end
  end

  describe Fields::DateField do
    it 'can convert string literals' do
      date = Fields::DateField.value_from_string '2001-02-03T04:05:06+07:00'
      expect(date).to eq DateTime.new(2001, 2, 3, 4, 5, 6, '+7').to_time
    end
  end

  describe Fields::ForeignKeyField do
    include_context 'entities'

    it 'can check if it points to an entity' do
      expect(tweet['User'].foreign_key_to? user).to be_truthy
    end
  end
end
