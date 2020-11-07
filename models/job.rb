# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'aka_name' do
    ID 'id'
    Integer 'person_id'
    String 'name'
    String 'imdb_index'
    String 'name_pcode_cf'
    String 'name_pcode_nf'
    String 'surname_pcode'
    String 'md5sum'
  end)

  (Entity 'aka_title' do
    ID 'id'
    Integer 'movie_id'
    String 'title'
    String 'imdb_index'
    Integer 'kind_id'
    Integer 'production_year'
    Integer 'phonetic_code'
    Integer 'episode_of_id'
    Integer 'season_nr'
    Integer 'episode_nr'
    String 'note'
    String 'md5sum'
  end)

  (Entity 'cast_info' do
    ID 'id'
    Integer 'person_id'
    Integer 'movie_id'
    Integer 'person_role_id'
    String 'note'
    Integer 'nr_order'
    Integer 'role_id'
  end)

  (Entity 'char_name' do
    ID 'id'
    String 'name'
    String 'imdb_index'
    String 'imdb_id'
    String 'name_pcode_nf'
    String 'surname_pcode'
    String 'md5sum'
  end)

  (Entity 'comp_cast_type' do
    ID 'id'
    String 'kind'
  end)

  (Entity 'company_name' do
    ID 'id'
    String 'name'
    String 'country_code'
    Integer 'imdb_id'
    String 'name_pcode_nf'
    String 'name_pcode_sf'
    String 'md5sum'
  end)

  (Entity 'company_type' do
    ID 'id'
    String 'kind'
  end)

  (Entity 'complete_cast' do
    ID 'id'
    Integer 'movie_id'
    Integer 'subject_id'
    Integer 'status_id'
  end)

  (Entity 'info_type' do
    ID 'id'
    String 'info'
  end)

  (Entity 'keyword' do
    ID 'id'
    String 'keyword'
    String 'phonetic_code'
  end)

  (Entity 'kind_type' do
    ID 'id'
    String 'kind'
  end)

  (Entity 'link_type' do
    ID 'id'
    String 'link'
  end)

  (Entity 'movie_companies' do
    ID 'id'
    Integer 'movie_id'
    Integer 'company_id'
    Integer 'company_type_id'
    String 'note'
  end)

  (Entity 'movie_info' do
    ID 'id'
    Integer 'movie_id'
    Integer 'info_type_id'
    String 'info'
    String 'note'
  end)

  (Entity 'movie_info_idx' do
    ID 'id'
    Integer 'movie_id'
    Integer 'info_type_id'
    String 'info'
    String 'note'
  end)

  (Entity 'movie_keyword' do
    ID 'id'
    Integer 'movie_id'
    Integer 'keyword_id'
  end)

  (Entity 'movie_link' do
    ID 'id'
    Integer 'movie_id'
    Integer 'linked_movie_id'
    Integer 'link_type_id'
  end)

  (Entity 'name' do
    ID 'id'
    String 'name'
    String 'imdb_index'
    String 'imdb_id'
    String 'gender'
    String 'name_pcode_cf'
    String 'name_pcode_nf'
    String 'surname_pcode'
    String 'md5sum'
  end)

  (Entity 'person_info' do
    ID 'id'
    Integer 'person_id'
    Integer 'info_type_id'
    String 'info'
    String 'note'
  end)

  (Entity 'role_type' do
    ID 'id'
    String 'role'
  end)

  (Entity 'title' do
    ID 'id'
    String 'title'
    String 'imdb_index'
    Integer 'kind_id'
    Integer 'production_year'
    Integer 'imdb_id'
    String 'phonetic_code'
    Integer 'episode_of_id'
    Integer 'season_nr'
    Integer 'episode_nr'
    String 'series_years'
    String 'md5sum'
  end)


end
