library(cora)
library(igraph)

# All record IDs in CORA (cora_gold refers to these row numbers)
all_ids <- as.character(seq_len(nrow(cora)))

# Edges from the gold pairs (ensure character ids)
edges <- data.frame(
  id1 = as.character(cora_gold$id1),
  id2 = as.character(cora_gold$id2),
  stringsAsFactors = FALSE
)

# Build an undirected graph with ALL ids as vertices (so singletons are included)
g <- graph_from_data_frame(edges, directed = FALSE,
                           vertices = data.frame(name = all_ids, stringsAsFactors = FALSE))

cmp <- components(g)

n_entities_total        <- cmp$no                     # total true entities (incl. singletons)
n_singleton_entities    <- sum(cmp$csize == 1)        # entities of size 1 (no duplicates)
n_multi_record_entities <- sum(cmp$csize >= 2)        # entities with duplicates
largest_entity_size     <- max(cmp$csize)

cat("Total true entities:", n_entities_total, "\n")
cat("Singleton entities  :", n_singleton_entities, "\n")
cat("Multi-record entities:", n_multi_record_entities, "\n")
cat("Largest entity size :", largest_entity_size, "\n")
