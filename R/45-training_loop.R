#' Train one epoch of the stable graph-neural ER model
#'
#' This routine:
#' 1) computes embeddings via the encoder,
#' 2) builds a kNN graph and a perturbed graph,
#' 3) runs graph clustering on both,
#' 4) constructs projection matrices,
#' 5) computes contrastive, graph smoothness, and stability losses,
#' 6) backpropagates into the encoder via the contrastive loss
#'    (graph and stability losses are treated as external regularizers here).
#'
#' @param encoder A RecordEncoder nn_module.
#' @param optimizer A torch optimizer.
#' @param X Torch tensor \code{c([n, p])} of input features.
#' @param pos_pairs Data frame of positive pairs (i, j).
#' @param neg_pairs Data frame of negative pairs (i, j).
#' @param lambda_graph Weight of the graph smoothness term.
#' @param lambda_K Weight of the stability term.
#' @param k_nn Number of neighbours in kNN graph.
#' @param perturb_cfg List with entries noise_sd, p_dropout.
#' @param clustering_method "leiden" or "louvain".
#'
#' @return A list with loss components.
#' @export
train_one_epoch <- function(encoder,
                            optimizer,
                            X,
                            pos_pairs,
                            neg_pairs,
                            lambda_graph = 1e-3,
                            lambda_K     = 1e-3,
                            k_nn         = 10,
                            perturb_cfg  = list(noise_sd = 0.01,
                                                p_dropout = 0.05),
                            clustering_method = "leiden") {
  encoder$train()
  optimizer$zero_grad()

  # 1) Forward embeddings
  Z_torch <- encoder(X)          # [n, d]
  Z       <- as.matrix(torch::as_array(Z_torch))

  # 2) Graphs
  g      <- build_knn_graph(Z, k = k_nn)
  L      <- graph_laplacian_matrix(g)

  g_del  <- perturb_graph(
    g,
    noise_sd  = perturb_cfg$noise_sd,
    p_dropout = perturb_cfg$p_dropout
  )
  L_del  <- graph_laplacian_matrix(g_del)

  # 3) Clustering and projections
  memb     <- run_graph_clustering(g,     method = clustering_method)
  memb_del <- run_graph_clustering(g_del, method = clustering_method)

  P     <- projection_from_membership(memb)
  P_del <- projection_from_membership(memb_del)

  # 4) Losses
  L_pair <- contrastive_loss(Z_torch, pos_pairs, neg_pairs, tau = 0.1)

  L_graph_val <- graph_smoothness_loss(Z, L)
  L_graph_t   <- torch::torch_tensor(
    as.numeric(L_graph_val),
    dtype = torch::torch_float(),
    requires_grad = FALSE
  )

  L_K_val <- stability_penalty(P, L, P_del, L_del, top_m = 10)
  L_K_t   <- torch::torch_tensor(
    as.numeric(L_K_val),
    dtype = torch::torch_float(),
    requires_grad = FALSE
  )

  loss <- L_pair + lambda_graph * L_graph_t + lambda_K * L_K_t

  loss$backward()
  optimizer$step()

  list(
    loss_total = as.numeric(loss$item()),
    loss_pair  = as.numeric(L_pair$item()),
    loss_graph = as.numeric(L_graph_val),
    loss_K     = as.numeric(L_K_val)
  )
}

#' Run training for multiple epochs
#'
#' High-level driver that trains the RecordEncoder and
#' returns the trained encoder plus a history of loss values.
#'
#' @param X_mat Numeric matrix \code{c([n, p])} of input features.
#' @param pos_pairs Data frame of positive pairs (i, j).
#' @param neg_pairs Data frame of negative pairs (i, j).
#' @param epochs Number of training epochs.
#' @param lr Learning rate for Adam.
#' @param hidden_dim Hidden layer size for encoder.
#' @param emb_dim Embedding dimension.
#' @param k_nn Number of neighbours in kNN graph.
#' @param lambda_graph Weight of graph smoothness loss.
#' @param lambda_K Weight of stability loss.
#'
#' @return A list with elements:
#'   - encoder: trained RecordEncoder
#'   - history: data.frame of loss values by epoch
#' @export
run_training <- function(X_mat,
                         pos_pairs,
                         neg_pairs,
                         epochs       = 50,
                         lr           = 1e-3,
                         hidden_dim   = 256,
                         emb_dim      = 64,
                         k_nn         = 10,
                         lambda_graph = 1e-3,
                         lambda_K     = 1e-3) {
  n <- nrow(X_mat)
  p <- ncol(X_mat)

  X_torch <- torch::torch_tensor(X_mat, dtype = torch::torch_float())

  encoder <- RecordEncoder(
    input_dim  = p,
    hidden_dim = hidden_dim,
    emb_dim    = emb_dim
  )
  optimizer <- torch::optim_adam(encoder$parameters, lr = lr)

  history <- data.frame(
    epoch      = integer(),
    loss_total = numeric(),
    loss_pair  = numeric(),
    loss_graph = numeric(),
    loss_K     = numeric()
  )

  for (e in seq_len(epochs)) {
    res <- train_one_epoch(
      encoder   = encoder,
      optimizer = optimizer,
      X         = X_torch,
      pos_pairs = pos_pairs,
      neg_pairs = neg_pairs,
      lambda_graph = lambda_graph,
      lambda_K     = lambda_K,
      k_nn         = k_nn
    )

    cat(sprintf(
      "Epoch %d: total=%.4f pair=%.4f graph=%.4f K=%.4f\n",
      e, res$loss_total, res$loss_pair,
      res$loss_graph, res$loss_K
    ))

    history <- rbind(history, data.frame(
      epoch      = e,
      loss_total = res$loss_total,
      loss_pair  = res$loss_pair,
      loss_graph = res$loss_graph,
      loss_K     = res$loss_K
    ))
  }

  list(
    encoder = encoder,
    history = history
  )
}
