

install.packages("usethis")
# (optionally, also install pkgdown, testthat, covr, etc.)
install.packages(c("pkgdown", "testthat", "covr", "microbenchmark", "profvis"))

library(usethis)

# Set globally for all repos:
use_git_config(
  user.name  = "Xinmin Chu",
  user.email = "Xinmin.Chu001@umb.edu"
)

usethis::use_git()             # initialize Git
usethis::use_git_ignore(c(".Rhistory", ".RData"))
usethis::use_readme_rmd(open = FALSE)
usethis::use_code_of_conduct(contact = "Xinmin Chu <Xinmin.Chu001@umb.edu>")

# This will scaffold DESCRIPTION, R/, man/, NAMESPACE, etc.,
# without moving you out of the folder:
usethis::create_package(path = ".", open = FALSE)


usethis::use_lifecycle_badge("Experimental")
usethis::use_news_md()

usethis::use_r("data_prep")

install.packages("devtools")
library(devtools)


devtools::document()
devtools::load_all()

?erbot_prep

install.packages("cora")
library(cora)
erbot_prep(cora[1:10,])

usethis::use_test("data_prep")

devtools::test()
