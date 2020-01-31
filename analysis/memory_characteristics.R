library(readr)
library(dplyr)
library(ggplot2)
library(scales)

function() {
	source("./memory_characteristics.R")

	d <- read_csv("memory_characteristics_min.csv")

	width <- 8
	height <- 5

	format_bytes <- label_bytes(units="auto_binary", accuracy=0.1)
	format_si <- label_number_si(accuracy=0.1)

	caption_text <- paste(
		"Evaluated by applying the MIN offline algorithm on 107.7M accesses.\n",
		"Size information is collected each 100,000 accesses.\n",
		"The irregularities at the end are due to swapping due to memory pressure\n",
		"caused by other processes on the same system."
	)

	d %>%
		ggplot() +
			geom_line(aes(x=access_index, y=state_bytes, colour="StatsCollector's State")) +
			geom_line(aes(x=access_index, y=rss_bytes, colour="Resident Size Set (RSS)")) +
			geom_line(aes(x=access_index, y=rss_bytes - state_bytes)) +
			scale_x_continuous(labels=format_si) +
			scale_y_continuous(breaks=extended_breaks(Q=c(1, 1024)), labels=format_bytes) +
			labs(
				x = "Access Index",
				y = "Bytes in Memory",
				colour = "",
				caption = caption_text
			)

	ggsave("memory_characteristics_stats.pdf", width=width, height=height)

	d %>%
		ggplot() +
			geom_line(aes(x=access_index, y=rss_bytes - state_bytes, colour="Non-StatsCollector's State")) +
			geom_line(aes(x=access_index, y=rss_bytes, colour="Resident Size Set (RSS)")) +
			scale_x_continuous(labels=format_si) +
			scale_y_continuous(breaks=extended_breaks(Q=c(1, 1024)), labels=format_bytes) +
			labs(
				x = "Access Index",
				y = "Bytes in Memory",
				colour = "",
				caption = caption_text
			)

	ggsave("memory_characteristics_non_stats.pdf", width=width, height=height)

}
