#' Neural record encoder using torch
#'
#' This module maps record-level feature vectors to low-dimensional
#' L2-normalized embeddings suitable for graph-based ER.
#'
#' @param input_dim Input feature dimension.
#' @param hidden_dim Hidden layer size.
#' @param emb_dim Embedding dimension.
#' @param dropout_p Dropout probability.
#' @return A torch nn_module generator. Call it with \code{input_dim} (and
#'   optionally \code{hidden_dim}, \code{emb_dim}, \code{dropout_p}) to create
#'   an encoder instance. Requires package \pkg{torch}.
#' @export
if (requireNamespace("torch", quietly = TRUE)) {
  RecordEncoder <- torch::nn_module(
    "RecordEncoder",

    initialize = function(input_dim,
                          hidden_dim = 256,
                          emb_dim    = 64,
                          dropout_p  = 0.1) {
      self$fc1     <- torch::nn_linear(input_dim, hidden_dim)
      self$fc2     <- torch::nn_linear(hidden_dim, emb_dim)
      self$dropout <- torch::nn_dropout(dropout_p)
      self$act     <- torch::nn_relu()
    },

    forward = function(x) {
      # x: [batch_size, input_dim]
      h      <- self$fc1(x)
      h      <- self$act(h)
      h      <- self$dropout(h)
      z      <- self$fc2(h)
      # L2 normalize embeddings
      z_norm <- torch::torch_norm(z, p = 2, dim = 2, keepdim = TRUE) + 1e-8
      z / z_norm
    }
  )
} else {
  RecordEncoder <- function(...) {
    stop(
      "Package 'torch' is required for RecordEncoder. ",
      "Install with: install.packages('torch')"
    )
  }
}

#' Simple contrastive loss for ER embeddings
#'
#' This is a basic InfoNCE-like contrastive loss with an extra penalty
#' that discourages high similarity for negative pairs.
#'
#' @param z Torch tensor of embeddings with shape \code{c([n, d])}.
#' @param pos_pairs Data frame with columns i, j (1-based indices) for positive pairs.
#' @param neg_pairs Data frame with columns i, j (1-based indices) for negative pairs.
#' @param tau Temperature parameter.
#'
#' @return A torch scalar loss.
#' @export
contrastive_loss <- function(z,
                             pos_pairs,
                             neg_pairs,
                             tau = 0.1) {
  # z: [n, d], assumed L2-normalized
  sim_mat <- torch::torch_mm(z, z$t())  # cosine similarity

  loss_pos <- torch::torch_tensor(0, dtype = torch::torch_float())
  if (nrow(pos_pairs) > 0) {
    for (k in seq_len(nrow(pos_pairs))) {
      i <- pos_pairs$i[k]
      j <- pos_pairs$j[k]
      s_ij <- sim_mat[i, j] / tau

      # denominator: similarities of i to all records
      denom <- torch::torch_logsumexp(sim_mat[i, ] / tau, dim = 1)
      loss_pos <- loss_pos - (s_ij - denom)
    }
    loss_pos <- loss_pos / nrow(pos_pairs)
  }

  loss_neg <- torch::torch_tensor(0, dtype = torch::torch_float())
  if (nrow(neg_pairs) > 0) {
    for (k in seq_len(nrow(neg_pairs))) {
      i <- neg_pairs$i[k]
      j <- neg_pairs$j[k]
      s_ij <- sim_mat[i, j]
      # penalize large similarity for negative pairs
      loss_neg <- loss_neg + torch::torch_relu(s_ij)
    }
    loss_neg <- loss_neg / nrow(neg_pairs)
  }

  loss_pos + loss_neg
}
