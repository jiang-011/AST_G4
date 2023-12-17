library(data.table)
library(lubridate)
library(readxl)
library(ggplot2)
library(clipr)


gc()
rm(list = ls())

# FUNCION DE AGRUPACION

agrupamiento_km <- function(Fecha_hora) {
  
  index_na <- which(is.na(Fecha_hora))
  for (i in index_na) {
    Fecha_hora[i] <- Fecha_hora[i - 1]
  }
  return(Fecha_hora)
}

# KILOMETROS

km <- fread("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/km_tp_st.csv", 
            dec = ",")

km[, Ficha := substr(Ficha, 2, 5)]
km[, Ficha := as.numeric(Ficha)]

km <- km[!(is.na(Ficha)),]
km <- km[!(is.na(Linea)),]
km <- km[!Linea == "",]

lineas_a_filtrar <- km[, .(cant_ficha= uniqueN(Ficha)), by = "Linea"]
lineas_a_filtrar <- lineas_a_filtrar[cant_ficha < 10, "Linea"]

km <- km[!Linea %in% lineas_a_filtrar[[1]],] #filtro las lineas con menos de 10 coches

km <- km[!(is.na(Fecha)),]
km[, Fecha := as.Date(Fecha)]
km[, Mes := floor_date(Fecha, "month")]

# Se filtran Unidades de Negocio que no son de interes
km_urbana <- km[UN == "Urbana"]
km_urbana <- km_urbana[Fecha > "2021-09-30",]
km_urbana <- km_urbana[Fecha <= "2023-10-31",]

km_dia <- km_urbana[, .(KM = sum(Km, na.rm = TRUE))
                    , by = .(Fecha)]

km_dia[,Fecha := as.Date(Fecha)]

cantidad_coches <- km_urbana[,.(Cantidad_coches = uniqueN(Ficha),
                                Cantidad_KM = sum(Km, na.rm = TRUE)), by = .(Mes)]

# CARGAS COMBUSTIBLE

lts <- fread("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/lts_tp_st.csv", 
             dec = ",")

lts[, Ficha := substr(Ficha, 2, 5)]

lts[, Ficha := as.numeric(Ficha)]

lts <- lts[!(is.na(Ficha)),]
lts <- lts[!(is.na(FechaCorregida)),]
lts[, FechaCorregida := as.Date(FechaCorregida)]

consumo <- merge.data.table(km, lts, 
                            by.x = c("Fecha", "Ficha"),
                            by.y = c("FechaCorregida", "Ficha"),
                            all = TRUE)

setorder(consumo, Ficha, -Fecha)

consumo[,Fecha_hora := agrupamiento_km(`Fecha Hora consumo`), by = "Ficha"]

consumo <- consumo[UN == "Urbana"]

lts_agrupado <- consumo[, .(LTS = sum(ConsumoTotal, na.rm = TRUE),
                            KM = sum(Km, na.rm = TRUE),
                            Linea = first(sort(Linea)),
                            UN = first(sort(UN)),
                            Texto = first(sort(Texto)),
                            Almacen = first(sort(CodAlmacen)),
                            Producto = first(sort(ProductName))
), 
by = .(Ficha, Fecha_hora)
]

lts_agrupado <- lts_agrupado[!is.na(Linea),]
lts_agrupado <- lts_agrupado[!is.na(Ficha),]

lts_agrupado <- lts_agrupado[!Linea == "",]

lts_agrupado[, Fecha := as.Date(Fecha_hora)]
lts_agrupado <- lts_agrupado[hour(Fecha_hora) < 6, Fecha := Fecha - 1]

lts_agrupado <- lts_agrupado[!is.na(Fecha),]

lts_agrupado <- lts_agrupado[UN == "Urbana"]
lts_agrupado <- lts_agrupado[Fecha > "2021-09-30",]
lts_agrupado <- lts_agrupado[Fecha <= "2023-10-31",]

lts_dia <- lts_agrupado[, .(LTS = sum(LTS, na.rm = TRUE),
                            KM = sum(KM, na.rm = TRUE)),
                        by = .(Fecha)]

lts_dia[, Consumo := LTS / KM * 100]

lts_dia_2 <- lts_dia[, c("Fecha", "Consumo")]

# PASAJEROS

pasajeros_1 <- read_xlsx("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/Pasajeros_2019_2020_2021.xlsx")
pasajeros_2 <- read_xlsx("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/Pasajeros_2022_2023.xlsx")

setDT(pasajeros_1)
setDT(pasajeros_2)

pasajeros <- rbindlist(list(pasajeros_1, pasajeros_2))

pasajeros <- pasajeros[!is.na(FechaRecaudacion),]
pasajeros[, FechaRecaudacion := as.Date(FechaRecaudacion)]

pasajeros <- pasajeros[!is.na(des_lin),]
pasajeros <- pasajeros[,-c("ID_Clase", "BanderaColor")]
pasajeros <- pasajeros[FechaRecaudacion > "2021-09-30",]
pasajeros <- pasajeros[FechaRecaudacion <= "2023-10-31",]

match_linea <- fread("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/match_linea.csv")

setnames(match_linea, "Linea", "Linea_corregida")

pasajeros <- merge.data.table(pasajeros, match_linea,
                              by.x = "des_lin",
                              by.y = "Lineas",
                              all.x = TRUE)

UN <- fread("C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/un.csv")

pasajeros <- merge.data.table(pasajeros, UN,
                              by.x = "Linea_corregida",
                              by.y = "Línea",
                              all.x = TRUE)

pasajeros <- pasajeros[des_lin == "130/146", UN := "Urbana"]
pasajeros <- pasajeros[UN == "Urbana",]

pasajeros_dia <- pasajeros[, .(Cantidad_Pasajeros = sum(Pasajeros, na.rm = TRUE)),
                           by = .(FechaRecaudacion)]

pasajeros_dia[, FechaRecaudacion := as.Date(FechaRecaudacion)]

# SE GRABAN LOS ARCHIVOS
fwrite(pasajeros_dia, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/Pasajeros_TP.csv")
fwrite(lts_dia_2, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/Consumo_TP.csv")
fwrite(km_dia, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/Kilometros_TP.csv")
fwrite(cantidad_coches, "C:/Users/rodri/OneDrive/Documentos/Maestría Ciencia de Datos/Series Temporales/Coches_mensuales.csv")


write_clip(cantidad_coches)


# gráfica
p1 <- ggplot(cantidad_coches) +
  geom_line(aes(x = Mes, y = Cantidad_coches))

p2 <- ggplot(cantidad_coches) +
  geom_line(aes(x = Mes, y = Cantidad_KM))

library(patchwork)

p1 + p2


