clusteredInstance <- read.csv("D:/CodeSpace/projectSpace/FBDP-zzq/homework7/output/clusteredInstances_k10i5.txt", header=FALSE)
View(clusteredInstance)
library(ggplot2)
library(RColorBrewer)
getPalette = colorRampPalette(brewer.pal(9, "Set1"))
clusteredInstance$cluster= as.character(clusteredInstance$V3)
ggplot(clusteredInstance, aes(V1, V2, color = cluster)) +
  geom_point(size=3)+
  scale_color_manual(values = getPalette(10))


