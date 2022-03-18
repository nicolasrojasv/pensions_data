# Web Scraping Pilar Solidario

#Limpiar ambiente
rm(list = ls())
options(scipen = 999)

#Librerías
sapply(c("data.table","compiler","lubridate","readxl","readr","janitor","tidyr","purrr","stringr","DBI","odbc",
         "keyring", "stringi"),
       require, character.only = T, quietly = T)

#Escritorio
setwd("//192.168.100.101/g/nicolas_rojas/viz_webpage/sist_pensiones_chileno/pilar_solidario")

#Cargar datos ## Los datos ya no se cargan desde CSV, sino que directamente de la base de SQL.
## Si es que la conexión falla, borrar los # y correr las tres líneas para cargar los datos en CSV. 
#sps_region <- fread("Inputs/sps_region.csv" , integer64 = "numeric")
#sps_edad <- fread("Inputs/sps_edad.csv", integer64 = "numeric")
#aps_institucion <- fread("Inputs/aps_institucion.csv", integer64 = "numeric")

#Conexión a SQL
con <- dbConnect(odbc(), 
                 Driver = "SQL Server", 
                 Server = "192.168.100.149",
                 Database = "pensiones",
                 trusted_connection = "yes",
                 uid = "estudios",
                 pdw = key_get("sql_password", "nicolas_rojas"), #Cambiar contraseña: script password
                 encoding = "latin1")

#Importar datos desde SQL
sps_region <- data.table(dbReadTable(con, "sps_region"))
sps_edad <- data.table(dbReadTable(con, "sps_edad"))
aps_institucion <- data.table(dbReadTable(con,"aps_institucion"))

#Fechas Información disponibles en la página del pilar solidario de la Súper de Pensiones
fechas_consulta <- gsub(pattern = "-|[0-9]{2}$", replacement = "", as.character(seq(from = as.Date("2008-07-01"), 
                                                                                    to = today(), 
                                                                                    by = "month")))

#############################################################
### DATOS DEL SISTEMA DE PENSIONES SOLIDARIAS POR REGION ####
#############################################################

#Función para descargar datos de beneficiarios y montos pagados por región
descargar_sps_region <- function(fechas, bd) {
  
  #Si la base de datos no existe en el ambiente, crear una vacía
  if (missing(bd)) {
    bd <- data.table(region = character(), tipo_beneficio = character(), sexo = character(), 
                     financiamiento = character(), num_beneficiarios = numeric(), mon_beneficiarios = numeric(),
                     num_region = numeric(), fecha = character())
  } else {
    bd <- bd
  }
  
  #Lista vacía para consolidar los datos
  consolidado <- list()
  
  #Iterar por fecha para extraer la información
  for (fecha in fechas) {
    
    #Saltar fechas que ya están en la base
    if (fecha %in% unique(gsub(pattern = "-|01$","",x = bd$fecha))) next
    
    #Índice para guardar las tablas
    i <- match(fecha,fechas)
    
    tryCatch({
      
      #Se crea un archivo temporal y se descargan los datos
      archivo_temporal <- tempfile(fileext = ".xls")
      url <- paste0("https://www.spensiones.cl/inf_estadistica/sps/nbmpm/",year(ym(fecha)),"/c02nbmpm",fecha,".xls")
      download.file(url, destfile = archivo_temporal, mode = "wb", quiet = T)
      
      if (as.numeric(fecha) > 202001) {
        
        #Leer los datos y transformarlos
        dt <- data.table(suppressMessages(read_excel(path = archivo_temporal, skip = 4)))[,c(8:13, 16:19), with = F]
        names(dt) <- c("region","tipo_beneficio","num_mujeres_fin_estatal","monto_mujeres_fin_estatal",
                       "num_hombres_fin_estatal","monto_hombres_fin_estatal","num_mujeres_fin_cuenta_ind",
                       "monto_mujeres_fin_cuenta_ind","num_hombres_fin_cuenta_ind","monto_hombres_fin_cuenta_ind")
        
        #Rellenar datos y eliminar los totales y partes vacías
        dt <- fill(dt, region)
        dt <- dt[!grepl("^Total",region)]
        dt <- dt[-c(120:125),]
        
        #Crear tabla financiamiento estatal para las mujeres
        dt_muj_est <- dt[,.(region,tipo_beneficio,num_mujeres_fin_estatal,monto_mujeres_fin_estatal)]
        dt_muj_est[,":="(sexo = "Mujer", financiamiento = "Estatal")]
        dt_muj_est <- dt_muj_est[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_mujeres_fin_estatal,
                                    mon_beneficio = monto_mujeres_fin_estatal)]
        
        #Crear tabla financiamiento estatal para los hombres
        dt_hom_est <- dt[,.(region,tipo_beneficio,num_hombres_fin_estatal,monto_hombres_fin_estatal)]
        dt_hom_est[,":="(sexo = "Hombre", financiamiento = "Estatal")]
        dt_hom_est <- dt_hom_est[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_hombres_fin_estatal,
                                    mon_beneficio = monto_hombres_fin_estatal)]
        
        #Crear tabla financiamiento cuenta individual para las mujeres
        dt_muj_ind <- dt[,.(region,tipo_beneficio,num_mujeres_fin_cuenta_ind,monto_mujeres_fin_cuenta_ind)]
        dt_muj_ind[,":="(sexo = "Mujer", financiamiento = "Cuenta Individual")]
        dt_muj_ind <- dt_muj_ind[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_mujeres_fin_cuenta_ind,
                                    mon_beneficio = monto_mujeres_fin_cuenta_ind)]
        
        #Crear tabla financiamiento cuenta individual para los hombres
        dt_hom_ind <- dt[,.(region,tipo_beneficio,num_hombres_fin_cuenta_ind,monto_hombres_fin_cuenta_ind)]
        dt_hom_ind[,":="(sexo = "Hombre", financiamiento = "Cuenta Individual")]
        dt_hom_ind <- dt_hom_ind[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_hombres_fin_cuenta_ind,
                                    mon_beneficio = monto_hombres_fin_cuenta_ind)]
        
        #Unir las tablas
        dt <- rbindlist(list(dt_muj_est, dt_hom_est, dt_muj_ind, dt_hom_ind))
        
        #Crear columnas fecha y numero de región
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        dt[, num_region := fcase(region == "Arica y Parinacota", 1,
                                 region == "Tarapacá", 2,
                                 region == "Antofagasta", 3,
                                 region == "Atacama", 4,
                                 region == "Coquimbo", 5,
                                 region == "Valparaíso", 6,
                                 region == "Metropolitana de Santiago", 7,
                                 region == "Libertador Gral. Bernardo O'Higgins", 8,
                                 region == "Maule", 9,
                                 region == "Ñuble", 10,
                                 region == "Biobío", 11,
                                 region == "La Araucanía", 12,
                                 region == "Los Ríos", 13,
                                 region == "Los Lagos", 14,
                                 region == "Aysén del Gral. Carlos Ibáñez del Campo", 15,
                                 region == "Magallanes y de la Antártica Chilena", 16,
                                 default = 0)]
        
        #Guardar los datos en el consolidado
        consolidado[[i]] <- dt
        
        #Tabla auxiliar para equiparar el nombre de las regiones (diferencias entre fechas de descarga)
        region <- unique(dt[,.(region,num_region)])
        region[, aux := toupper(stri_trans_general(str = region, id = "Latin-ASCII"))]
        region[, aux := fcase(grepl("LIBERTADOR", aux), "O'HIGGINS", 
                              grepl("BIOBIO", aux), "BIO BIO", 
                              grepl("^LA A", aux), "ARAUCANIA", 
                              grepl("AYSEN", aux), "AYSEN", 
                              grepl("MAGA", aux), "MAGALLANES", 
                              grepl("METRO", aux), "METROPOLITANA", 
                              grepl("NUBLE", aux), "CHILLÁN",
                              grepl("SIN INFORMACION", aux), "Sin Información",
                              rep_len(TRUE, length(aux)), aux)]
        region_nuble <- data.table(region = "Ñuble", num_region = 10, aux = "ÑUBLE")
        region <- rbindlist(list(region, region_nuble), use.names = T)
        
      } else if (as.numeric(fecha) %between% c(201905,202001)) {
        
        #Leer los datos y renombrar las columnas
        dt <- data.table(suppressMessages(read_excel(path = archivo_temporal, skip = 4)))[,c(1:6),with = F]
        names(dt) <- c("region","tipo_beneficio","num_mujeres_fin_estatal","monto_mujeres_fin_estatal",
                       "num_hombres_fin_estatal","monto_hombres_fin_estatal")
        
        #Rellenar las columnas y eliminar datos vacíos
        dt <- fill(dt, region)
        dt <- dt[!grepl("^Total|^Fuente|^Notas|^\\(",region)]
        
        #Crear tabla de financiamiento estatal para las mujeres
        dt_muj_est <- dt[,.(region,tipo_beneficio,num_mujeres_fin_estatal,monto_mujeres_fin_estatal)]
        dt_muj_est[,":="(sexo = "Mujer", financiamiento = "Estatal")]
        dt_muj_est <- dt_muj_est[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_mujeres_fin_estatal,
                                    mon_beneficio = monto_mujeres_fin_estatal)]
        
        #Crear tabla de financiamiento estatal para los hombres
        dt_hom_est <- dt[,.(region,tipo_beneficio,num_hombres_fin_estatal,monto_hombres_fin_estatal)]
        dt_hom_est[,":="(sexo = "Hombre", financiamiento = "Estatal")]
        dt_hom_est <- dt_hom_est[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_hombres_fin_estatal,
                                    mon_beneficio = monto_hombres_fin_estatal)]
        
        #Unir las tablas
        dt <- rbindlist(list(dt_muj_est, dt_hom_est))
        
        #Crear columna fecha y número de región
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        dt[, num_region := fcase(region == "Arica y Parinacota", 1,
                                 region == "Tarapacá", 2,
                                 region == "Antofagasta", 3,
                                 region == "Atacama", 4,
                                 region == "Coquimbo", 5,
                                 region == "Valparaíso", 6,
                                 region == "Metropolitana de Santiago", 7,
                                 region == "Libertador Gral. Bernardo O'Higgins", 8,
                                 region == "Maule", 9,
                                 region == "Ñuble", 10,
                                 region == "Biobío", 11,
                                 region == "La Araucanía", 12,
                                 region == "Los Ríos", 13,
                                 region == "Los Lagos", 14,
                                 region == "Aysén del Gral. Carlos Ibáñez del Campo", 15,
                                 region == "Magallanes y de la Antártica Chilena", 16,
                                 default = 0)]
        
        #Guardar los datos en el consolidado
        consolidado[[i]] <- dt
        
      } else {
        
        #Leer los datos y renombrar las columnas
        dt <- data.table(suppressMessages(read_excel(path = archivo_temporal, skip = 6)))[,c(1:6), with = F]
        names(dt) <- c("region","tipo_beneficio","num_mujeres_fin_estatal","monto_mujeres_fin_estatal",
                       "num_hombres_fin_estatal","monto_hombres_fin_estatal")
        
        #Rellenar los datos de región y eliminar los totales
        dt <- fill(dt,region)
        dt <- dt[!grepl("^TOTAL|^Total|^\\(",region)]
        dt[, aux := trimws(gsub("[0-9]{1,}", "", region))][,region := NULL]
        dt[, aux := ifelse(is.na(aux), "SIN INFORMACION DE REGION", aux)]
        dt <- merge.data.table(dt,region, by = "aux", all.x = T)
        
        #Crear tabla de financiamiento estatal para mujeres
        dt_muj_est <- dt[,.(region,tipo_beneficio,num_mujeres_fin_estatal,monto_mujeres_fin_estatal,num_region)]
        dt_muj_est[,":="(sexo = "Mujer", financiamiento = "Estatal")]
        dt_muj_est <- dt_muj_est[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_mujeres_fin_estatal,
                                    mon_beneficio = monto_mujeres_fin_estatal, num_region)]
        
        #Crear tabla de financiamiento estatal para hombres
        dt_hom_est <- dt[,.(region,tipo_beneficio,num_hombres_fin_estatal,monto_hombres_fin_estatal,
                                                    num_region)]
        dt_hom_est[,":="(sexo = "Hombre", financiamiento = "Estatal")]
        dt_hom_est <- dt_hom_est[,.(region,tipo_beneficio,sexo,financiamiento,
                                    num_beneficiarios = num_hombres_fin_estatal,
                                    mon_beneficio = monto_hombres_fin_estatal, num_region)]
        
        #Unir las tablas
        dt <- rbindlist(list(dt_muj_est, dt_hom_est), use.names = T)
        
        #Crear columna fecha
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        
        #Guardar los datos en el consolidado
        consolidado[[i]] <- dt
        
      }
    }, error = print
    )
  }
  return(rbindlist(consolidado, use.names = T))
}
descargar_sps_region <- cmpfun(descargar_sps_region)

#Descargar datos del sistema de pensiones solidarias por región
if (exists("sps_region")) {
  
  sps_region_nuevo <- descargar_sps_region(rev(fechas_consulta), sps_region)
  
} else {
  
  sps_region_nuevo <- descargar_sps_region(rev(fechas_consulta))
  
}

try({
  
  #Limpiar variables categóricas y missing values pasarlos a cero
  sps_region_nuevo[, ":="(tipo_beneficio = trimws(gsub("\\([0-9]\\)", "", tipo_beneficio)),
                   num_beneficiarios = ifelse(is.na(num_beneficiarios), 0, num_beneficiarios),
                   mon_beneficio = ifelse(is.na(mon_beneficio), 0, mon_beneficio))]
  
  #Complemento de trabajo pesado tiene solo valores iguales a cero. Se elimina.
  sps_region_nuevo <- sps_region_nuevo[!grepl("Complemento", tipo_beneficio),
                                       .(region, tipo_beneficio, sexo, financiamiento, num_beneficiarios, 
                                         mon_beneficio, num_region, fecha)]
  
}, silent = T)

#Unir los nuevos datos con el consolidado
if (exists("sps_region")) {
  
  sps_region <- rbindlist(list(sps_region_nuevo, sps_region), use.names = T)
  
} else {
  
  sps_region <- copy(sps_region_nuevo)
  
}
rm(sps_region_nuevo)

#Guardar datos
write.csv2(sps_region, "Inputs/sps_region.csv", row.names = F, na = "")

#Guardar la tabla de datos en la base de datos SQL pensiones
dbWriteTable(con, "sps_region", sps_region, overwrite = T)

##########################################################
### DATOS DEL SISTEMA DE PENSIONES SOLIDARIAS POR EDAD ###
##########################################################

#Función para obtener los beneficiarios sps por edad
descargar_sps_edad <- function(fechas, bd) {
  
  #Si la base de datos no existe en el ambiente, crear una vacía
  if (missing(bd)) {
    
    bd <- data.table(sexo = character(), tramo_edad = character(), financiamiento = character(),
                     tipo_beneficio = character(), mon_beneficio = numeric(), num_beneficiarios = numeric(),
                     fecha = character())
    
  } else {
    
    bd <- bd
    
  }
  
  #Crear lista vacía para ir guardando los datos
  consolidado <- list()
  
  #Iterar por cada fecha para ir descargando la información
  for (fecha in fechas) {
    
    #Saltar las fechas que ya han sido descargadas
    if (fecha %in% unique(gsub(pattern = "-|01$","",x = bd$fecha))) next
    
    #Genera un indice que sirve para ir guardando las tablas en orden y que no se sobreescriban
    i <- match(fecha,fechas)
    
    tryCatch({
      
      #Crear un archivo temporal en donde se descarga el excel
      archivo_temporal <- tempfile(fileext = ".xls")
      url <- paste0("https://www.spensiones.cl/inf_estadistica/sps/nbmpm/",year(ym(fecha)),"/c06nbmpm",fecha,".xls")
      download.file(url, destfile = archivo_temporal, mode = "wb", quiet = T)
      
      
      if (as.numeric(fecha) >= 202002) {
        
        #Se crea el nombre de las columnas de la base final
        head1 <- gsub(pattern = "\\...[0-9]{1,}$",NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 2, col_names = T))))
        head2 <- gsub("...[0-9]{1,}$","", gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                               names(suppressMessages(read_excel(archivo_temporal, 
                                                                                 skip = 3, col_names = T)))))
        head3 <- gsub("...[0-9]{1,}$","",gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                              names(suppressMessages(read_excel(archivo_temporal, 
                                                                                skip = 4, col_names = T)))))
        for (n in 1:length(head1)) {
          if (is.na(head3[n])) {
            head3[n] <- head1[n]
            head1[n] <- NA_character_
          }
        }
        
        head1 <- fill(data.table(head1),head1)[,head1]
        head2 <- fill(data.table(head2),head2)[,head2]
        head3 <- fill(data.table(head3),head3)[,head3]
        
        #Nombre de columnas definitivo
        headers <- map_chr(1:length(head1), ~ {
          ifelse(!is.na(head1[.x]) & !is.na(head2[.x]) & !is.na(head3[.x]), 
                 paste(head1[.x], head2[.x], head3[.x], sep = "_"),
                 head3[.x])
        })
        
        #Se leen los datos y se les asigna el nombre de las columnas
        dt <- data.table(read_excel(archivo_temporal, skip = 5, col_names = headers))
        
        #Se cambia el nombre de los atributos de la variable sexo y se rellena la tabla 
        dt[,Sexo := ifelse(Sexo == "Mujeres", "Mujer",ifelse(Sexo == "Hombres", "Hombre",Sexo))]
        dt <- fill(dt, Sexo)
        
        #Se cambia el nombre de las columnas sexo y tramo de edad
        setnames(dt, old = c("Sexo","Tramo de edad (4)"), new = c("sexo","tramo_edad"))
        
        #Se ordena la tabla de lo ancho a lo largo, se eliminan los totales y se separan algunas variables
        dt <- melt(dt, id.vars = c(1:2), variable.factor = F)
        dt <- dt[grepl("Mujer|Hombre",sexo)][!grepl("total|Total",variable)]
        dt <- separate(dt, variable, into = c("financiamiento","tipo_beneficio","numero_monto"), sep = "_")
        dt[,":="(financiamiento = ifelse(financiamiento == "Con financiamiento estatal",
                                         "Estatal","Cuenta Individual"),
                 numero_monto = ifelse(numero_monto == "Número", "num_beneficiarios", "mon_beneficio"))]
        
        #Se pasa la tabla de lo largo a lo ancho para separar los datos de número y monto
        dt <- dcast(dt, ... ~ numero_monto, value.var = "value")
        
        #Se crea la fecha de la información
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        
        #se guarda la tabla en el consolidado
        consolidado[[i]] <- dt
        
        #Se guarda la tabla de febrero de 2020 para que la variable tramos de edad sea igual en todas las fechas
        if (as.numeric(fecha) == 202002) {
          aux1 <- consolidado[[i]]  
        }  
        
      } else if (as.numeric(fecha) %between% c(201905,202001)) {
        
        #Se crea el nombre de las columnas de la tabla
        head1 <- gsub(pattern = "\\...[0-9]{1,}$",NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 3, col_names = T))))
        head2 <- gsub(pattern = "\\...[0-9]{1,}$",NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 4, col_names = T))))
        head3 <- gsub("...[0-9]{1,}$","", gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                               names(suppressMessages(read_excel(archivo_temporal, 
                                                                                 skip = 5, col_names = T)))))
        head4 <- gsub("...[0-9]{1,}$","",gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                              names(suppressMessages(read_excel(archivo_temporal, 
                                                                                skip = 6, col_names = T)))))
        for (n in 1:length(head2)) {
          if (is.na(head3[n])) {
            head3[n] <- head2[n]
            head2[n] <- NA_character_
          }
        }
        
        head4[1] <- "tramo_edad"
        head1 <- fill(data.table(head1),head1)[,head1 := ifelse(head1 == "Mujeres","Mujer",
                                                                ifelse(head1 == "Hombres","Hombre",head1))][,head1]
        head3 <- fill(data.table(head3),head3)[,head3]
        head4 <- fill(data.table(head4),head4)[,head4]
        
        #Nombre final de las columnas
        headers <- map_chr(1:length(head1), ~ {
          ifelse(!is.na(head1[.x]) & !is.na(head3[.x]) & !is.na(head4[.x]), 
                 paste(head1[.x], head3[.x], head4[.x], sep = "_"),
                 head4[.x])
        })
        
        #Se leen los datos y se usa el vector headers como nombre de las columnas 
        dt <- data.table(suppressMessages(read_excel(archivo_temporal, skip = 7, col_names = headers)))
        
        #Se excluyen las filas que no son de interés y se pasa la tabla de lo ancho a lo largo, excluyendo totales
        dt <- dt[grepl("\\-|\\+|^Sin", tramo_edad)]
        dt <- melt(dt, id.vars = 1, variable.factor = F)
        dt <- dt[!grepl("total|Total",variable)]
        
        #Se separan las variables y se pasa la tabla de lo largo a lo ancho para separar número de monto
        dt <- separate(dt, variable, into = c("sexo","tipo_beneficio","numero_monto"), sep = "_")
        dt[,":="(financiamiento = "Estatal",
                 numero_monto = ifelse(numero_monto == "Número", "num_beneficiarios", "mon_beneficio"))]
        dt <- dcast(dt, ... ~ numero_monto, value.var = "value")
        
        #Se crea la variable fecha y se igual la variable tipo de beneficio a las tablas más actuales
        dt[, ":="(fecha = paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-"),
                  tipo_beneficio = ifelse(tipo_beneficio == "APS Vejez subsidio definido",
                                          "APS Vejez Subsidio Definido", tipo_beneficio))]
        
        #Se guardan los datos en el consolidado
        consolidado[[i]] <- dt
        
        #Se guarda la tabla de mayo de 2019 para igualar la variable tipo de beneficio de fechas anteriores
        if (as.numeric(fecha) == 201905) {
          aux2 <- consolidado[[i]]  
        }  
        
      } else {
        
        #Se crea el nombre de las columnas
        head1 <- gsub(pattern = "\\...[0-9]{1,}$",NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 4, col_names = T))))
        head2 <- gsub(pattern = "\\...[0-9]{1,}$",NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 5, col_names = T))))
        head3 <- gsub("...[0-9]{1,}$","", gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                               names(suppressMessages(read_excel(archivo_temporal, 
                                                                                 skip = 6, col_names = T)))))
        
        for (n in 1:length(head2)) {
          if (is.na(head3[n])) {
            head3[n] <- head1[n]
            head1[n] <- NA_character_
          }
        }
        
        head1[2] <- NA_character_
        head1 <- fill(data.table(head1),head1)[,head1]
        head2 <- fill(data.table(head2),head2)[,head2]
        head3 <- fill(data.table(head3),head3)[,head3]
        
        #Nombre definitivo de las columnas
        headers <- map_chr(1:length(head1), ~ {
          ifelse(!is.na(head1[.x]) & !is.na(head2[.x]) & !is.na(head3[.x]), 
                 paste(head1[.x], head2[.x], head3[.x], sep = "_"),
                 ifelse(!is.na(head2[.x]) & !is.na(head3[.x]), paste(head2[.x], head3[.x], sep = "_"),head3[.x]))
        })
        headers[1] <- "tramo_edad"
        
        #Se leen los datos y se pasa el vector headers para el nombre de la columna
        dt <- data.table(suppressMessages(read_excel(archivo_temporal, skip = 7, col_names = headers)))
        
        #Igualar la columna tramo de edad con la versión de fechas más actuales
        dt <- dt[grepl("\\-|\\+|^SIN|^HASTA|^MAS", tramo_edad)]
        tramos_edad <- aux1[,.(tramo_edad = unique(tramo_edad))]
        tramos_edad[,tramo_edad := ifelse(tramo_edad == "+100", "- 20",
                                          ifelse(tramo_edad == "- 20", "+100", tramo_edad))]
        dt$tramo_edad <- tramos_edad$tramo_edad
        
        #Pasar la tabla de ancho a largo, eliminar los totales y separar las columnas
        dt <- melt(dt, id.vars = 1, variable.factor = F)
        dt <- dt[!grepl("^TOTALES",variable)]
        dt <- separate(dt, variable, into = c("tipo_beneficio","numero_monto"), sep = "_")
        dt[,":="(financiamiento = "Estatal",
                 numero_monto = ifelse(numero_monto == "NÚMERO", "num_beneficiarios", "mon_beneficio"))]
        
        #Pasar la tabla de largo al ancho para serar los números de los montos
        dt <- dcast(dt, ... ~ numero_monto, value.var = "value")
        
        #Hacer coincidir la columna tipo de beneficio con los valores de fechas más actuales
        dt[,tipo_beneficio := tolower(tipo_beneficio)]
        dt <- merge.data.table(dt, 
                               aux2[,.(tipo_beneficio = gsub("ó","o",tolower(unique(tipo_beneficio))),
                                       tipo_beneficio_bien = unique(tipo_beneficio))], 
                               by = "tipo_beneficio", all.x = T)
        dt[,tipo_beneficio := tipo_beneficio_bien][,tipo_beneficio_bien := NULL]
        
        #Crear la variable fecha
        dt[, ":="(fecha = paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-"),
                  sexo = "Sin información")]
        
        #Guardar la tabla de datos en el consolidado
        consolidado[[i]] <- dt
        
      }
    }, error = print
    )
  }
  
  #Transformar la lista de tabla en una sola
  consolidado <- rbindlist(consolidado, use.names = T)
  
  #Si es que existe el consolidado, arreglar la columna tramo de edad
  if (length(consolidado) > 0) {
    consolidado[, tramo_edad := gsub("^\\-","\\<",tramo_edad)]
    consolidado[, tramo_edad := ifelse(grepl("\\+100",tramo_edad), gsub("^\\+","\\>\\=",tramo_edad),
                                                tramo_edad)]
    consolidado[, tramo_edad := gsub("^\\+","\\[",tramo_edad)]
    consolidado[, tramo_edad := ifelse(!grepl("^Menor|^Mayor|^Sin",tramo_edad),
                                                gsub(" \\- ",", ",tramo_edad),tramo_edad)]
    consolidado[, tramo_edad := ifelse(grepl("\\[",tramo_edad), paste0(tramo_edad,")"),tramo_edad)]
    consolidado[, ":="(mon_beneficio = as.numeric(mon_beneficio), 
                       num_beneficiarios = as.numeric(num_beneficiarios))]
  }
  return(consolidado)
}
descargar_sps_edad <- cmpfun(descargar_sps_edad)

#Descargar datos del sistema de pensiones solidarias por edad
if (exists("sps_edad")) {
  
  sps_edad_nuevo <- descargar_sps_edad(rev(fechas_consulta), sps_edad)
  
} else {
  
  sps_edad_nuevo <- descargar_sps_edad(rev(fechas_consulta))
  
}

try({
  
  #Limpiar variables categóricas
  sps_edad_nuevo[, ":="(tipo_beneficio = trimws(gsub("\\([0-9]\\)", "", tipo_beneficio)),
                        num_beneficiarios = ifelse(is.na(num_beneficiarios), 0, num_beneficiarios),
                        mon_beneficio = ifelse(is.na(mon_beneficio), 0, mon_beneficio))]
  
})

#Unir los nuevos datos con el consolidado
if (exists("sps_edad")) {
  
  sps_edad <- rbindlist(list(sps_edad_nuevo, sps_edad), use.names = T)
  
} else {
  
  sps_edad <- copy(sps_edad_nuevo)
  
}
rm(sps_edad_nuevo)

#Guardar datos
write.csv2(sps_edad, "Inputs/sps_edad.csv", row.names = F)

#Guardar la tabla de datos en la base de datos SQL pensiones
dbWriteTable(con, "sps_edad", sps_edad, overwrite = T)

############################################################
### DATOS DE APORTES SOLIDARIOS POR INSTITUCIÓN PAGADORA ###
############################################################

#Función para extraer datos del APS por institución
descargar_aps_institucion <- function(fechas, bd) {
  
  #Si la base de datos no existe en el ambiente, crear una vacía
  if (missing(bd)) {
    
    bd <- data.table(tipo_beneficio = character(), sexo = character(), institucion = character(), 
                     mon_beneficio = numeric(), num_beneficiarios = numeric(), fecha = character())
    
  } else {
    
    bd <- bd
    
  }
  
  #crear lista vacía para ir guardando los datos
  consolidado <- list()
  
  #Iterara cada fecha para ir descargando los datos
  for (fecha in fechas) {
    
    #Saltar las fechas que ya han sido descargadas
    if (fecha %in% unique(gsub(pattern = "-|01$","",x = bd$fecha))) next
    
    #Generar un índice para guardar en orden las tablas en el consolidado sin sobreescribir una encima de la otra
    i <- match(fecha,fechas)
    
    tryCatch({
      
      #Crear un archivo temporal en donde se guardan los datos descargados
      archivo_temporal <- tempfile(fileext = ".xls")
      url <- paste0("https://www.spensiones.cl/inf_estadistica/sps/nbmpm/",year(ym(fecha)),"/c07nbmpm",fecha,".xls")
      download.file(url, destfile = archivo_temporal, mode = "wb", quiet = T)
      
      if (as.numeric(fecha) >= 202002) {
        
        #Crear las columnas de la tabla
        head1 <- gsub(pattern = "\\...[0-9]{1,}$", NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 2, col_names = T))))
        head1[1] <- "tramo_edad"
        head2 <- gsub("...[0-9]{1,}$","", gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                               names(suppressMessages(read_excel(archivo_temporal, 
                                                                                 skip = 3, col_names = T)))))
        head3 <- gsub("...[0-9]{1,}$","",gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                              names(suppressMessages(read_excel(archivo_temporal, 
                                                                                skip = 4, col_names = T)))))
        head3 <- gsub(pattern = " \\([0-9]\\)$", "", head3)
        
        for (n in 1:length(head1)) {
          if (is.na(head3[n])) {
            head3[n] <- head1[n]
            head1[n] <- NA_character_
          } 
        }
        
        head1 <- fill(data.table(head1),head1)[,head1]
        head2 <- fill(data.table(head2),head2)[,head2]
        head3 <- fill(data.table(head3),head3)[,head3]
        
        #Nombres finales de las columnas
        headers <- map_chr(1:length(head1), ~ {
          ifelse(!is.na(head1[.x]) & !is.na(head2[.x]) & !is.na(head3[.x]), 
                 paste(head1[.x], head2[.x], head3[.x], sep = "_"),
                 head3[.x])
        })
        
        #Leer los datos y asignar el vector headers al nombre de las columnas
        dt <- data.table(read_excel(archivo_temporal, skip = 5, col_names = headers))
        
        #Sacar filas innecesarias, pasar la tabla de ancho a largo y crear las columnas relevantes
        dt <- dt[grepl("\\-|\\+|^Sin", tramo_edad)]
        dt <- melt(dt, id.vars = 1, variable.factor = F)
        dt <- separate(dt, variable, into = c("sexo","institucion","numero_monto"), sep = "_")
        
        #Eliminar los totales de la tabla y cambiar el nombre de algunos atributos
        dt <- dt[!grepl("Total",sexo)][!grepl("Total", institucion)]
        dt[,":="(numero_monto = ifelse(numero_monto == "Número", "num_beneficiarios", "mon_beneficio"),
                 sexo = ifelse(sexo == "Hombres","Hombre", "Mujer"))]
        
        #Pasar la tabla de largo a ancho para generar las columnas monto y numero de beneficiarios
        dt <- dcast(dt, ... ~ numero_monto, value.var = "value")
        
        #Crear la columna fecha
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        
        #Guardar los datos en el consolidado
        consolidado[[i]] <- dt
        
        #Se guarda la tabla de febrero de 2020 para que la variable tramos de edad sea igual en todas las fechas
        if (as.numeric(fecha) == 202002) {
          aux1 <- consolidado[[i]]  
        }  
        
      } else if (as.numeric(fecha) %between% c(201905,202001)) {
        
        #Encontrar el nombre de las columnas
        head1 <- gsub(pattern = "\\...[0-9]{1,}$", NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 3, col_names = T))))
        head2 <- gsub("...[0-9]{1,}$","", gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                               names(suppressMessages(read_excel(archivo_temporal, 
                                                                                 skip = 4, col_names = T)))))
        head3 <- gsub("...[0-9]{1,}$","",gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                              names(suppressMessages(read_excel(archivo_temporal, 
                                                                                skip = 5, col_names = T)))))
        head4 <- gsub("...[0-9]{1,}$","",gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                              names(suppressMessages(read_excel(archivo_temporal, 
                                                                                skip = 6, col_names = T)))))
        head4[1] <- "tramo_edad"
        
        head1 <- fill(data.table(head1),head1)[,head1]
        head2 <- fill(data.table(head2),head2)[,head2]
        head3 <- fill(data.table(head3),head3)[,head3]
        
        #Nombre definitivo de las columnas
        headers <- map_chr(1:length(head1), ~ {
          ifelse(!is.na(head1[.x]) & !is.na(head2[.x]) & !is.na(head3[.x]) & !is.na(head4[.x]), 
                 paste(head1[.x], head2[.x], head3[.x], head4[.x], sep = "_"),
                 head4[.x])
        })
        
        #Leer los datos y asigar el nombre de las columnas según el vector headers
        dt <- data.table(read_excel(archivo_temporal, skip = 7, col_names = headers))
        
        #Eliminar filas innecesarias, pasar la tabla de lo ancho a lo largo para crear columnas relevantes
        dt <- dt[grepl("\\-|\\+|^Sin", tramo_edad)]
        dt <- melt(dt, id.vars = 1, variable.factor = F)
        dt <- separate(dt, variable, into = c("sexo","aux","institucion","numero_monto"), sep = "_")
        
        #Eliminar los totales y columnas innecesaria
        dt <- dt[!grepl("Total", sexo)][!grepl("Total", aux)]
        dt[, aux := NULL]
        
        #Cambiar el nombre de atributos de algunos variables
        dt[,":="(numero_monto = ifelse(numero_monto == "Número", "num_beneficiarios", "mon_beneficio"),
                 sexo = ifelse(sexo == "Hombres","Hombre", "Mujer"))]
        
        #Pasar la tabla de largo a ancho para difenciar monto de número de beneficiarios
        dt <- dcast(dt, ... ~ numero_monto, value.var = "value")
        
        #Crear la fecha de la tabla
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        
        #Guardar la tabla en el consolidado
        consolidado[[i]] <- dt
        
      } else {
        
        #Encontrar el nombre de las columnas de la tabla 
        head1 <- gsub(pattern = "\\...[0-9]{1,}$", NA_character_,
                      names(suppressMessages(read_excel(archivo_temporal, skip = 4, col_names = T))))
        head2 <- gsub("...[0-9]{1,}$","", gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                               names(suppressMessages(read_excel(archivo_temporal, 
                                                                                 skip = 5, col_names = T)))))
        head3 <- gsub("...[0-9]{1,}$","",gsub(pattern = "^...[0-9]{1,}$",NA_character_,
                                              names(suppressMessages(read_excel(archivo_temporal, 
                                                                                skip = 6, col_names = T)))))
        
        head1 <- fill(data.table(head1),head1)[,head1]
        head2 <- fill(data.table(head2),head2)[,head2]
        head3 <- fill(data.table(head3),head3)[,head3]
        
        for (n in 1:length(head1)) {
          if (is.na(head3[n])) {
            head3[n] <- head1[n]
            head1[n] <- NA_character_
          } 
        }
        
        #nombre definitivo de las columnas 
        headers <- map_chr(1:length(head1), ~ {
          ifelse(!is.na(head1[.x]) & !is.na(head2[.x]) & !is.na(head3[.x]), 
                 paste(head1[.x], head2[.x], head3[.x], sep = "_"),
                 head3[.x])
        })
        headers[1] <- "tramo_edad"
        
        #Leer los datos y asignar el vector headers como el nombre de las columnas
        dt <- data.table(read_excel(archivo_temporal, skip = 7, col_names = headers))
        
        #Eliminar filas innecesarias y se transforman los tramos de edad según los datos de las fechas más actuales
        dt <- dt[grepl("\\+|^HASTA|^MAS|^SIN", tramo_edad)]
        tramos_edad <- aux1[,.(tramo_edad = unique(tramo_edad))]
        tramos_edad[,tramo_edad := ifelse(tramo_edad == "+100", "- 20",
                                          ifelse(tramo_edad == "- 20", "+100", tramo_edad))]
        dt$tramo_edad <- tramos_edad$tramo_edad
        
        #Pasar la tabla del ancho a lo largo y crear las columnas relevantes
        dt <- melt(dt, id.vars = 1, variable.factor = F)
        dt <- separate(dt, variable, into = c("aux","institucion","numero_monto"), sep = "_")
        
        #Eliminar los totlaes y borrar columna innecesaria
        dt <- dt[!grepl("TOTALES", aux)]
        dt[, aux := NULL]
        
        #Cambiar el nombre los atributos de algunas variables
        dt[,":="(numero_monto = ifelse(numero_monto == "NÚMERO", "num_beneficiarios", "mon_beneficio"),
                 sexo = "Sin Información")]
        
        #Pasar la tabla de lo largo a lo ancho para separa las columnas monto y número
        dt <- dcast(dt, ... ~ numero_monto, value.var = "value")
        
        #Crear la variable fecha
        dt[, fecha := paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-")]
        
        #modificar la variable institución para asemejarla a los datos de fechas actuales
        dt[, institucion := ifelse(!grepl("AFP|IPS|ISL", institucion), 
                                   trimws(gsub(" D", " d",gsub(", Ley", "", str_to_title(institucion)))), 
                                   institucion)]
        
        #Guardar la tabla en el consolidado
        consolidado[[i]] <- dt
        
      }
    }, error = print
    )
  }
  
  #Pasar el consolidado de una lista de tablas a una base de datos consolidada
  consolidado <- rbindlist(consolidado, use.names = T)
  
  #Si el consolidado existe, modificar la variable tramo de edad
  if (length(consolidado) > 0) {
    consolidado[, tramo_edad := gsub("^\\-","\\<",tramo_edad)]
    consolidado[, tramo_edad := ifelse(grepl("\\+100",tramo_edad), gsub("^\\+","\\>\\=",tramo_edad),
                                       tramo_edad)]
    consolidado[, tramo_edad := gsub("^\\+","\\[",tramo_edad)]
    consolidado[, tramo_edad := ifelse(!grepl("^Menor|^Mayor|^Sin",tramo_edad),
                                                gsub(" \\- ",", ",tramo_edad),tramo_edad)]
    consolidado[, tramo_edad := ifelse(grepl("\\[",tramo_edad), paste0(tramo_edad,")"),tramo_edad)]
    consolidado[, ":="(mon_beneficio = as.numeric(mon_beneficio), 
                       num_beneficiarios = as.numeric(num_beneficiarios))]
  }
  return(consolidado)
}

descargar_aps_institucion <- cmpfun(descargar_aps_institucion)

#Descargar datos de aps por institución
if (exists("aps_institucion")) {
  
  aps_institucion_nuevo <- descargar_aps_institucion(rev(fechas_consulta), aps_institucion)
  
} else {
  
  aps_institucion_nuevo <- descargar_aps_institucion(rev(fechas_consulta))
  
}

try({
  
  #Limpiar variables
  aps_institucion_nuevo[, ":="(mon_beneficio = ifelse(is.na(mon_beneficio), 0, mon_beneficio),
                               num_beneficiarios = ifelse(is.na(num_beneficiarios), 0, num_beneficiarios))]
  aps_institucion_nuevo <- aps_institucion_nuevo[!grepl("Otra",institucion)]
  
  #Transformar los tramos de edad en APS Invalidez (menores de 65) y Vejez (mayores de 65)
  aps_institucion_nuevo[, tipo_beneficio := ifelse(grepl("100|\\[65|75|85|95", tramo_edad), "APS Vejez",
                                                   ifelse(grepl("Sin", tramo_edad), "Sin información", 
                                                          "APS Invalidez"))]
  aps_institucion_nuevo <- aps_institucion_nuevo[,.(tipo_beneficio,sexo,institucion,mon_beneficio,num_beneficiarios,
                                                    fecha)]
  
})

#Unir los nuevos datos con el consolidado
if (exists("aps_institucion")) {
  
  aps_institucion <- rbindlist(list(aps_institucion_nuevo, aps_institucion), use.names = T)
  
} else {
  
  aps_institucion <- copy(aps_institucion_nuevo)
  
}
rm(aps_institucion_nuevo)

#Guardar datos
write.csv2(aps_institucion, "Inputs/aps_institucion.csv", row.names = F, na = "")

#Guardar datos en la base relacional SQL pensiones
dbWriteTable(con, "aps_institucion", aps_institucion, overwrite = T)

#Desconectar la conexión a SQL
dbDisconnect(con)
