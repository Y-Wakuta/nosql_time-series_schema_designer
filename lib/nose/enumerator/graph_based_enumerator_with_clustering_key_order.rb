# frozen_string_literal: true

require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class GraphBasedIndexEnumeratorWithClusteringKeyOrder < GraphBasedIndexEnumerator

    # In this enumerator, we do not ignore suffix clustering key attribute order for comparison with the full-functioning proposed method (GraphBasedIndexEnumerator)
    def ignore_cluster_key_order(query, indexes)
      return indexes
    end
  end
end
