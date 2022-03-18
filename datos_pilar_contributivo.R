# Web Scraping Pilar Contributivo

#Limpiar ambiente
rm(list = ls())
options(scipen = 999)

#Librerías
sapply(c("data.table","httr","rvest","compiler","RSelenium","lubridate","readxl","purrr", "stringr", "tidyr",
         "DBI", "odbc", "keyring", "readr"),
       require, character.only = T, quietly = T)

#Escritorio
setwd("//192.168.100.101/g/nicolas_rojas/viz_webpage/sist_pensiones_chileno/pilar_contributivo")

#Cargar datos. Los datos se cargan desde la base de datos SQL. Si es que se cae, cargarlos desde las carpetas
#afiliados_tipo <- fread("Inputs/afiliados_tipo.csv", integer64 = "numeric")
#afiliados_saldo <- fread("Inputs/afiliados_saldo.csv", integer64 = "numeric")
#cotizantes_fondo <- fread("Inputs/cotizantes_fondo.csv", integer64 = "numeric")
#cotizantes_afp <- fread("Inputs/cotizantes_afp.csv", integer64 = "numeric")

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
afiliados_tipo <- data.table(dbReadTable(con, "afiliados_tipo"))
afiliados_saldo <- data.table(dbReadTable(con, "afiliados_saldo"))
cotizantes_fondo <- data.table(dbReadTable(con, "cotizantes_fondo"))
cotizantes_afp <- data.table(dbReadTable(con, "cotizantes_afp"))
serie_uf <- data.table(dbReadTable(con, "serie_uf"))

#fechas para las descargas
fechas_consulta <- gsub(pattern = "-|[0-9]{2}$", replacement = "", as.character(seq(from = as.Date("2002-12-01"), 
                                                                                 to = today(), 
                                                                                 by = "month")))
#fechas para descargar los datos de la uf
fecha_uf <- gsub(pattern = "-[0-9]{2}", replacement = "", as.character(seq(from = as.Date("1977-01-01"),
                                                                           to = today(), 
                                                                           by = "year")))

#######################################
### COTIZANTES E INGRESO IMPONIBLE ####
#######################################

#Función para descargar datos de cotizantes del sistema de pensiones
descargar_cotizantes <- function() {
  
  #Descargar los datos en una carpeta temporal
  archivo_temporal <- tempfile(fileext = ".xls")
  url <- paste0("https://www.spensiones.cl/inf_estadistica/series_afp/cotizantes/cotizantes_ingreso_imponible_promedio.xls")
  download.file(url, destfile = archivo_temporal, mode = "wb", quiet = T)
  
  #Leer la tabla de datos
  dt <- data.table(suppressMessages(read_excel(path = archivo_temporal, skip = 1, sheet = 1,
                                               col_types = c("date","text","numeric","numeric","numeric","numeric",
                                                             "numeric","numeric","numeric","numeric","numeric","numeric",
                                                             "numeric","numeric","numeric","numeric","numeric","numeric",
                                                             "numeric","numeric","numeric","numeric","numeric","numeric",
                                                             "numeric","numeric","numeric","numeric","numeric","numeric",
                                                             "numeric","numeric","numeric","numeric"))))
  
  #Cambiar nombres de las variables
  dt <- dt[,.(fecha = Fecha, afp = AFP, n_dependientes_total = `N° de Cotizantes Dependientes`,
              n_dependientes_hombres = `N° de Cotizantes Dependientes Masculino`, 
              n_dependientes_mujeres = `N° de Cotizantes Dependientes Femenino`, 
              n_dependientes_sininfo = `N° de Cotizantes Dependientes sin información de sexo`, 
              n_independientes_total = `N° de Cotizantes Independientes`, 
              n_independientes_hombres = `N° de Cotizantes Independientes Masculino`,
              n_independientes_mujeres = `N° de Cotizantes Independientes Femenino`,
              n_independientes_sininfo = `N° de Cotizantes Independientes sin información de sexo`,
              n_voluntarios_total = `N° de Cotizantes Afil. voluntarios`, 
              n_voluntarios_hombres = `N° de Cotizantes Afil. voluntarios Masculino`,
              n_voluntarios_mujeres = `N° de Cotizantes Afil. voluntarios Femenino`,
              n_voluntarios_sininfo = `N° de Cotizantes Afil. voluntarios sin información de sexo`,
              n_total_ambossexo = `N° de Cotizantes`,
              n_total_hombres = `N° de Cotizantes  Masculino`,
              n_total_mujeres = `N° de Cotizantes  Femenino`, 
              n_total_sininfo = `N° de Cotizantes  sin información de sexo`,
              ing_dependientes_total = `Ing. Imp. Prom. Cotizantes Dependientes`,
              ing_dependientes_hombres = `Ing. Imp. Prom. Cotizantes Dependientes Masculino`,
              ing_dependientes_mujeres = `Ing. Imp. Prom. Cotizantes Dependientes Femenino`,
              ing_dependientes_sininfo = `Ing. Imp. Prom. Cotizantes Dependientes sin información de sexo`,
              ing_independientes_total = `Ing. Imp. Prom. Cotizantes Independientes`,
              ing_independientes_hombres = `Ing. Imp. Prom. Cotizantes Independientes Masculino`,
              ing_independientes_mujeres = `Ing. Imp. Prom. Cotizantes Independientes Femenino`,
              ing_independientes_sininfo = `Ing. Imp. Prom. Cotizantes Independientes sin información de sexo`,
              ing_voluntarios_total = `Ing. Imp. Prom. Cotizantes Afil. voluntarios`,
              ing_voluntarios_hombres = `Ing. Imp. Prom. Cotizantes Afil. voluntarios Masculino`,
              ing_voluntarios_mujeres = `Ing. Imp. Prom. Cotizantes Afil. voluntarios Femenino`,
              ing_voluntarios_sininfo = `Ing. Imp. Prom. Cotizantes Afil. voluntarios sin información de sexo`,
              ing_total_ambossexo = `Ing. Imp. Prom. Cotizantes`,
              ing_total_hombres =  `Ing. Imp. Prom. Cotizantes  Masculino`,
              ing_total_mujeres = `Ing. Imp. Prom. Cotizantes  Femenino`,
              ing_total_sininfo = `Ing. Imp. Prom. Cotizantes  sin información de sexo`)]
  
  #Pasar la tabla de ancho a largo y armar las columnas de interés
  dt <- melt(dt, id.vars = c(1,2), variable.name = "factor", value.name = "num_monto")
  dt <- separate(dt, col = "factor", into = c("valor", "tipo_cotizante", "sexo"), sep = "_")
  
  #Eliminar los datos que no son necesarios. Ejemplo: Totales
  dt <- dt[!grepl("^total",tipo_cotizante)][!(grepl("^total|^ambossexo", sexo))][!grepl("^TOTAL",afp)]
  
  #renombrar variables y variables categóricas y eliminar columnas no requeridas
  dt[, ":="(valor = ifelse(valor == "n", "num_cotizantes","ing_prom"),
            fecha = as.character(fecha),
            afp = str_to_title(afp),
            sexo = ifelse(sexo == "sininfo", "s/i", sexo))]
  
  #Pasar la tabla del largo a lo ancho para separar las columnas monto y número
  dt <- dcast(dt, formula = fecha + afp + tipo_cotizante + sexo ~ valor, value.var = "num_monto")
  
  #Pasar a ceros los missing value
  #dt[, ":="(ing_prom = ifelse(is.na(ing_prom), 0, ing_prom),
  #          num_cotizantes = ifelse(is.na(num_cotizantes), 0, num_cotizantes))]
  
  return(dt)
}
descargar_cotizantes <- cmpfun(descargar_cotizantes)

#Descargar serie de cotizantes
cotizantes_afp <- descargar_cotizantes()

#Pasar a NA valores igual a 0
cotizantes_afp[, ":="(ing_prom = ifelse(ing_prom == 0, NA_integer_, ing_prom),
                      num_cotizantes = ifelse(num_cotizantes == 0, NA_integer_, num_cotizantes))]

#Información trimestral (por ahora se ocupa la información mensual)
#cotizantes <- cotizantes[grepl("\\b3\\b|\\b6\\b|\\b9\\b|\\b12\\b", month(fecha))]

#Guardar datos en la unidad de red
write.csv2(cotizantes_afp, file = "Inputs/cotizantes_afp.csv", row.names = F, na = "")

#Guardar datos en base sql
dbWriteTable(con, "cotizantes_afp", cotizantes_afp, overwrite = T)

###############################################
### COTIZANTES POR EDAD, TIPO, SEXO y FONDO ###
###############################################

#Función para descargar cotizantes
descargar_cotizantes_fondo <- function(fechas, bd) {
  
  if (missing(bd)) {
    bd <- data.table(sexo = character(), tramo_edad = character(), num_cotizantes = numeric(), 
                     tipo_cotizante = character(), tipo_fondo = character(), fecha = character())
  } else {
    bd <- bd
  }
  
  #Lista vacía para guardar los datos
  consolidado <- list()
  
  #Conexión remota a Chrome
  remDr <- remoteDriver(remoteServerAddr = "192.168.100.34", port = 4445L, browserName = "chrome")
  remDr$open()
  
  #Loop para ir fecha por fecha descargando la información
  for (fecha in fechas) {
    if (fecha %in% unique(gsub(pattern = "-|01$","",x = bd$fecha))) next
    
    #Auxiliar para determinar la posición en donde se guardan los datos
    i <- match(fecha,fechas)
    
    #Navegar a la información de número de cotizantes por tipo de fondo
    remDr$navigate("https://www.spensiones.cl/apps/centroEstadisticas/paginaCuadrosCCEE.php?menu=sci&menuN1=cotycot&menuN2=tipfon")
    
    tryCatch({
      
      #Encontrar la fecha de la descarga y hacer click
      webelem <- remDr$findElement(using = "css", paste0("[value = 'inf_estadistica/aficot/trimestral/",
                                                         year(ym(fecha)),
                                                         "/",substr(fecha,5,6),"/17A']"))
      webelem$clickElement()
      
      #Entrar a la información por edad, tipo y sexo (n°5)
      webelem <- remDr$findElements(using = "css", paste0("[value = 'Ver']"))
      webelem[[5]]$clickElement()
      
      #Nombres de las columnas
      columns <- c("sexo", "< 20", "[20-25)", "[25-30)", "[30-35)", "[35-40)", "[40-45)", "[45-50)", "[50-55)",
                   "[55-60)", "[60-65)", "[65-70)", ">=70", "sin info", "total")
      
      #Función para extraer las tabla según el tipo de fondo
      extraer_tablas <- function(fondo) {
        
        #Variable auxiliar para definir la tabla a extraer
        if (fecha < 201609) {
          a <- if (fondo == "A") {
            3
          } else if (fondo == "B") {
            5
          } else if (fondo == "C") {
            7
          } else if (fondo == "D") {
            9
          } else if (fondo == "E") {
            11
          } else {
            "error"
          }
        } else {
          a <- if (fondo == "A") {
            1
          } else if (fondo == "B") {
            2
          } else if (fondo == "C") {
            3
          } else if (fondo == "D") {
            4
          } else if (fondo == "E") {
            5
          } else {
            "error"
          }
        }
        
        #Leer datos
        if (fecha <= 200809) {
          tabla <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), fill = T, header = T)[[a]][2:9,])
          
        } else {
          tabla <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), fill = T, header = T)[[a]][2:13,])
        }
        
        #Asignar el nombre de las columnas
        names(tabla) <- columns
        
        #Ordenar la columna tramo de edad
        tabla <- melt(tabla, id.vars = "sexo", variable.name = "tramo_edad", value.name = "num_cotizantes",
                      variable.factor = F)
        
        #Crear columna tipo de cotizante
        tabla[, tipo_cotizante := ifelse(sexo == "DEPENDIENTES", "dependientes", 
                                         ifelse(sexo == "INDEPENDIENTES", "independientes", 
                                                ifelse(sexo == "VOLUNTARIOS", "voluntarios", NA_character_)))]
        
        #Rellenar la columna tipo de cotizante
        tabla <- fill(tabla, tipo_cotizante)
        
        #Eliminar observaciones innecesarias
        tabla <- tabla[!grepl("DEPENDIENTES|VOLUNTARIOS", sexo)]
        
        #Cambiar de genero a sexo
        tabla[, sexo := ifelse(sexo == "Masculino", "hombres", ifelse(sexo == "Femenino", "mujeres", sexo))]
        
        #Eliminar los totales
        tabla <- tabla[!grepl("total", tramo_edad)]
        
        #Crear la fecha, el fondo y pasar a dato numerico el número de cotizantes
        tabla[, ":="(tipo_fondo = fondo, fecha = paste(year(ym(fecha)), substr(fecha,5,6), "01",sep = "-"),
                     num_cotizantes = as.numeric(gsub("\\.", "", num_cotizantes)))]
        
        return(tabla)
        
      }
      
      #Fondos
      fondos <- c("A", "B", "C", "D", "E")
      
      #Extraer datos de cotizantes
      lista_cotizante <- lapply(fondos, extraer_tablas)
      
      #Unir las tablas de datos
      datos_cotizante <- rbindlist(lista_cotizante)
      
      #Guardar datos
      consolidado[[i]] <- datos_cotizante
      
    }, error = print)
  }
  return(rbindlist(consolidado, use.names = T))
  
  #Cerrar conexión remota
  remDr$close()

}

#Descargar serie de cotizantes por fondo
if (exists("cotizantes_fondo")) {
  
  cotizantes_fondo_nuevo <- descargar_cotizantes_fondo(fechas = grep("03$|06$|09$|12$", fechas_consulta, value = T),
                                                       bd = cotizantes_fondo)
  
} else {
  
  cotizantes_fondo_nuevo <- descargar_cotizantes_fondo(fechas = grep("03$|06$|09$|12$", fechas_consulta, value = T))
  
}

if (nrow(cotizantes_fondo_nuevo) != 0) {
  
  #Cambiar la clase del tramo de edad
  cotizantes_fondo_nuevo[, tramo_edad := as.character(tramo_edad)]

}

#Unir nuevos datos con existentes
if (exists("cotizantes_fondo")) {
  
  cotizantes_fondo <- rbindlist(list(cotizantes_fondo, cotizantes_fondo_nuevo), use.names = T)
  
} else {
  
  cotizantes_fondo <- copy(cotizantes_fondo_nuevo)
}
rm(cotizantes_fondo_nuevo)

#Pasar a NA los valores iguales a 0
cotizantes_fondo[, ":="(num_cotizantes = ifelse(num_cotizantes == 0, NA_integer_, num_cotizantes))]

#Guardar datos
write.csv2(cotizantes_fondo, "Inputs/cotizantes_fondo.csv", row.names = F, na = "")

#Guardar datos en base sql
dbWriteTable(con, "cotizantes_fondo", cotizantes_fondo, overwrite = T)

############################
### AFILIADOS POR SALDO ####
############################

#Función para descargar afiliados por tipo de fondo
descargar_afiliados_saldo <- function(fechas, bd) {
  
  if (missing(bd)) {
    bd <- data.table(tramo_edad = character(), tramo_saldo_miles = character(), num_afiliados = numeric(),
                     fecha = character())
  } else {
    bd <- bd
  }
  
  #Lista vacía para guardar los datos
  consolidado <- list()
  
  #Conexión remota a Chrome
  remDr <- remoteDriver(remoteServerAddr = "192.168.100.34", port = 4445L, browserName = "chrome")
  remDr$open()
  
  #Loop para ir fecha por fecha descargando la información
  for (fecha in fechas) {
    if (fecha %in% unique(gsub(pattern = "-|01$","",x = bd$fecha))) next
    
    #Auxiliar para determinar la posición en donde se guardan los datos
    i <- match(fecha,fechas)
    
    #Navegar a la información de número de cotizantes por tipo de fondo
    remDr$navigate("https://www.spensiones.cl/apps/centroEstadisticas/paginaCuadrosCCEE.php?menu=sci&menuN1=afil&menuN2=sdomovcci")
    
    tryCatch({
      
      #Encontrar la fecha de la descarga y hacer click
      webelem <- remDr$findElement(using = "css", paste0("[value = 'inf_estadistica/aficot/trimestral/",
                                                         year(ym(fecha)),
                                                         "/",substr(fecha,5,6),"/09A']"))
      webelem$clickElement()
      
      #Entrar a la información
      webelem <- remDr$findElements(using = "css", paste0("[value = 'Ver']"))
      webelem[[8]]$clickElement()
      
      #Nombres de las columnas
      columns <- c("tramo_saldo_miles", "< 20", "[20-25)", "[25-30)", "[30-35)", "[35-40)", "[40-45)", "[45-50)",
                   "[50-55)", "[55-60)", "[60-65)", "[65-70)", ">=70", "sin info")
      
      #Leer datos
      if (fecha <= 201606) {
        dt <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), fill = T, header = T)[[1]][4:27,1:14])
        
      } else {
        dt <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), fill = T, header = T)[[1]][2:25,1:14])
      }
      
      #Asignar el nombre de las columnas
      names(dt) <- columns
      
      #Ordenar la columna tramo de edad
      dt <- melt(dt, id.vars = "tramo_saldo_miles", variable.name = "tramo_edad", value.name = "num_afiliados")
      
      #Crear la fecha, el fondo y pasar a dato numerico el número de cotizantes
      dt[, ":="(fecha = paste(year(ym(fecha)), substr(fecha,5,6), "01", sep = "-"), 
                num_afiliados = as.numeric(gsub("\\.", "", num_afiliados)))]
      
      #Guardar datos
      consolidado[[i]] <- dt
      
    }, error = print)
  }
  return(rbindlist(consolidado, use.names = T))
  
  #Cerrar conexión remota
  remDr$close()
  
}

#Descargar serie de cotizantes por fondo
if (exists("afiliados_saldo")) {
  
  afiliados_saldo_nuevo <- descargar_afiliados_saldo(fechas = grep("03$|06$|09$|12$", fechas_consulta, value = T),
                                                     bd = afiliados_saldo)
  
} else {
  
  afiliados_saldo_nuevo <- descargar_afiliados_saldo(fechas = grep("03$|06$|09$|12$", fechas_consulta, value = T))
  
}

if (ncol(afiliados_saldo_nuevo != 0)) {
  
  #Pasar a NA los valores iguales a 0
  afiliados_saldo_nuevo[, ":="(num_afiliados = ifelse(num_afiliados == 0, NA_integer_, num_afiliados),
                               tramo_edad = as.character(tramo_edad))]
  
  #Acortar los tramos de los saldos
  afiliados_saldo_nuevo[, tramo_saldo := fcase(
    tramo_saldo_miles == "CERO", "cero",
    grepl("5$|10$|20$|50$|100$", tramo_saldo_miles), "($0, $100M]",
    grepl("200$|500$", tramo_saldo_miles), "($100M, $500M]", 
    grepl("1000$", tramo_saldo_miles), "($500M, $1MM]",
    grepl("2000$|3000$|4000$|-5000$", gsub(" ","",tramo_saldo_miles)), "($1MM, $5MM]",
    grepl("7000$|10000$", tramo_saldo_miles), "($5MM, $10MM]",
    grepl("15000$|20000$", tramo_saldo_miles), "($10MM, $20MM]",
    grepl("30000$|40000$", tramo_saldo_miles), "($20MM, $40MM]",
    grepl("50000$|60000$", tramo_saldo_miles), "($40MM, $60MM]",
    grepl("80000$", tramo_saldo_miles), "($60MM, $80MM]",
    grepl("-100000$", tramo_saldo_miles), "($80MM, $100MM]", 
    default =  "más de $100MM")]
  
  #Sumar los afiliados según los nuevos tramos
  afiliados_saldo_nuevo <- afiliados_saldo_nuevo[, .(num_afiliados = sum(num_afiliados, na.rm = T)), 
                                                 by =.(fecha, tramo_edad, tramo_saldo)]
  
}

#Actualizar base de datos
if (exists("afiliados_saldo")) {
  
  afiliados_saldo <- rbindlist(list(afiliados_saldo, afiliados_saldo_nuevo), use.names = T)
  
} else {
  
  afiliados_saldo <- copy(afiliados_saldo_nuevo)
  
}
rm(afiliados_saldo_nuevo)

#Guardar datos
write.csv2(afiliados_saldo, "Inputs/afiliados_saldo.csv", row.names = F, na = "")

#Guardar datos en base sql
dbWriteTable(con, "afiliados_saldo", afiliados_saldo, overwrite = T)

#######################################
### AFILIADOS POR TIPO, SEXO y AFP ####
#######################################

descargar_afiliados_tipo <- function(fechas) {
  
  #Crear un archivo temporal para guardar el excel que se descarga
  archivo_temporal <- tempfile(fileext = ".xls")
  
  #URL de donde se descarga la información
  url <- paste0("https://www.spensiones.cl/inf_estadistica/series_afp/afiliados/afiliados_tipo_sexo_afp.xls")
  
  #Descargar datos en el archivo temporal
  download.file(url, destfile = archivo_temporal, mode = "wb", quiet = T)
  
  #Leer los datos
  dt <- data.table(suppressMessages(read_excel(path = archivo_temporal, skip = 1)))[c(1:961),]
  
  #Pasar las columnas correspondientes a numéricas
  cols <- grep("^VOL|^Vol",names(dt), value = T)
  dt[, (cols) := lapply(.SD, function(x) as.numeric(gsub("-", 0, x))), .SDcols = cols]
  
  #Eliminar los totales del sistema
  dt <- dt[!grepl("Sistema", AFP)]
  
  #Cambiar el nombre de las variables y seleccionarlas
  dt <- dt[,.(fecha = as.character(PERIODO), afp = AFP, dependientes_hombres = `Dep Masculino`, 
              dependientes_mujeres = `Dep Femenino`, dependientes_sininfo = `Dep S/I`, 
              independientes_hombres = `Indep Masculino`, independientes_mujeres = `Indep  Femenino`,
              independientes_sininfo = `Indep  S/I`, voluntarios_hombres = `Voluntarios Masculino`, 
              voluntarios_mujeres = `Voluntarios  Femenino`, voluntarios_sininfo = `Voluntarios  S/I`)]
  
  #Pasar la tabla de ancho a largo
  dt <- melt(dt, id.vars = c(1:2), variable.name = "factor", value.name = "num_afiliados", variable.factor = F)
  
  #Separar columnas de tipo de afiliado y sexo
  dt <- separate(dt, "factor", into = c("tipo_afiliado", "sexo"), sep = "_")
  
  #Modificar las columnas sexo y fecha
  dt[, ":="(sexo = gsub("sininfo", "s/i", sexo))]
            #fecha = gsub("-[0-9]{2}$", "", fecha))]
  
  #Segunda fuente de información para datos faltantes
  #Lista vacía para guardar los datos
  consolidado <- list()
  
  #Conexión remota a Chrome
  remDr <- remoteDriver(remoteServerAddr = "192.168.100.34", port = 4445L, browserName = "chrome")
  remDr$open()
  
  #Si por alguna razón no se carga la primera fuente de información 
  if (missing(dt) == T) dt <- data.table(fecha = character(), afp = character(), 
                                         tipo_afiliado = character(), sexo = character(), 
                                         num_afiliados = integer())
  
  #Loop para ir fecha por fecha descargando la información
  for (fecha in fechas) {
    
    #No descargar información que ya se descargó
    if (fecha %in% unique(gsub("-|01$","",x = dt$fecha))) next
    
    #índice para guardar las tablas en el consolidado
    i <- match(fecha, fechas)
    
    #Navegar a la página web
    remDr$navigate("https://www.spensiones.cl/apps/centroEstadisticas/paginaCuadrosCCEE.php?menu=sci&menuN1=afil&menuN2=afp")
    
    tryCatch({
      
      #Encontrar la fecha de la descarga y hacer click
      webelem <- remDr$findElement(using = "css", paste0("[value = 'inf_estadistica/aficot/trimestral/",
                                                         year(ym(fecha)),
                                                         "/",substr(fecha,5,6),"/02F']"))
      webelem$clickElement()
      
      #Entrar a la sección correspondiente
      webelem <- remDr$findElements(using = "css", paste0("[value = 'Ver']"))
      webelem[[3]]$clickElement()
      
      #Valores auxiliares para descargar bien la información. Es distinto dependiendo de si es > o < a junio 2016
      if (fecha > 201606) {
        a <- 1
        b <- 2
      } else {
        a <- 2
        b <- 3
      }
      
      #Encabezados
      head1 <- as.character(as.matrix(html_table(read_html(remDr$getPageSource()[[1]]), 
                                                 dec = ",", fill = T)[[a]])[a,])
      head1[1] <- NA_character_ ; head1 <- tolower(head1)
      head2 <- as.character(as.matrix(html_table(read_html(remDr$getPageSource()[[1]]), 
                                                 dec = ",", fill = T)[[a]])[b,])
      head2 <- gsub("\\.", "", head2) ; head2 <- tolower(head2)
      head2 <- gsub("masculino", "hombres", head2) ; head2 <- gsub("femenino", "mujeres", head2)
      
      headers <- map_chr(1:length(head1), ~ {
        ifelse(!is.na(head1[.x]) & !is.na(head2[.x]), 
               paste(head1[.x], head2[.x], sep = "_"),
               head2[.x])
      })
      
      #Descargar los datos
      dt2 <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), dec = ",", fill = T, header = F)[[a]])
      
      #Seleccionar filas y columnas
      dt2 <- dt2[c(4:nrow(dt2))][!grepl("^Total",X1)]
      
      #Cambiar el nombre de las columnas
      names(dt2) <- headers
      
      #Transformar los valores que corresponden a número
      cols <- names(dt2)[c(2:ncol(dt2))]
      dt2[, (cols) := lapply(.SD, function(x) as.numeric(gsub("\\.", "", x))), 
          .SDcols = cols][, afp := str_to_title(afp)]
      
      #Transformar la tabla de ancho a largo
      dt2 <- melt(dt2, id.vars = 1, variable.name = "factor", value.name = "num_afiliados")
      
      #Separar variables sexo y tipo de afiliado
      dt2 <- separate(dt2, col = "factor", into = c("tipo_afiliado", "sexo"), sep = "_")
      
      #Eliminar los totales
      dt2 <- dt2[!grepl("^total", tipo_afiliado)][!grepl("^total", sexo)]
      
      #Crear columna fecha
      dt2[, fecha := paste(year(ym(fecha)), substr(fecha, 5, 6), "01",sep = "-")]
      
      #Guardar la tabla en el consolidado
      consolidado[[i]] <- dt2
      
    }, error = print)
  }
  
  #Cerrar conexión
  remDr$close()
  
  #Retornar la base de datos
  return(rbindlist(list(dt,rbindlist(consolidado, use.names = T)), use.names = T))
  
}
descargar_afiliados_tipo <- cmpfun(descargar_afiliados_tipo) 

#Descargar serie de afiliados por tipo
afiliados_tipo <- descargar_afiliados_tipo(grep("03$|06$|09$|12$", fechas_consulta, value = T))

#Pasar valores cero a missing values
afiliados_tipo[, num_afiliados := ifelse(num_afiliados == 0, NA_integer_, num_afiliados)]

#Guardar datos
write.csv2(afiliados_tipo, "Inputs/afiliados_tipo.csv", row.names = F, na = "")

#Guardar datos en base sql
dbWriteTable(con, "afiliados_tipo", afiliados_tipo, overwrite = T)

################
### SERIE UF ###
################

#Función para extraer la UF del Banco Central
extraer_uf <- function(fechas) {
  
  #Crear lista vacía para ir guardando las series anuales
  series_uf <- vector("list", tail(fechas, n = 1))
  
  #Abrir sesión remota de Chrome
  remDr <- remoteDriver(remoteServerAddr = "192.168.100.34", port = 4445L, browserName = "chrome")
  remDr$open()
  
  #Navegar por la Base de Datos Estadísticos del Banco Central
  remDr$navigate("https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_PRECIOS/MN_CAP_PRECIOS/UF_IVP_DIARIO")
  
  for (fecha in fechas) {
    tryCatch({
      #Crear un índice para ir guardando los data table
      i <- match(fecha, fechas)
      
      #Buscar el año para extraer la serie
      webelem <- remDr$findElement(using = "css", paste0("[value = '",fecha,"']"))
      webelem$clickElement()
      
      #Extraer la serie de UF
      serie_uf <- data.table(html_table(html_nodes(read_html(remDr$getPageSource()[[1]]), xpath = "//table")[[2]]))
      
      #Ordenar la serie
      tidy_serie <- melt(serie_uf, id.vars = c(1,2), variable.name = "periodo", 
                         value.name = "valor")[,.(Serie, periodo, valor)]
      tidy_serie <- dcast(tidy_serie, formula = periodo ~ Serie, 
                          value.var = "valor")[,.(periodo, uf = `Unidad de fomento (UF)`)]
      tidy_serie <- tidy_serie[,":="(fecha = parse_date(gsub("sep","sept",tolower(sub("\\.", " ", periodo))), 
                                                        "%d %b%Y", locale = locale("es")),
                                     uf = as.numeric(gsub(",",".", gsub("\\.","",uf))))]
      
      #Guardar data table en la lista
      series_uf[[i]] <- tidy_serie
      
    }, error = print)
  }
  remDr$close()
  return(rbindlist(series_uf, use.names = T))
}
extraer_uf <- cmpfun(extraer_uf)

#Extraer las series de UF
series_uf <- extraer_uf(fecha_uf)

#Armar serie con la última fecha del mes y quedarse solo con el año y mes en la columna periodo
series_uf <- series_uf[,.(uf = tail(uf, n = 1)), by =.(fecha = paste(year(fecha), substr(fecha,6,7), "01",
                                                                     sep = "-"))]
#Incluir auxiliar para pesos
series_uf[, pesos := 1]

#Pasar la tabla a lo largo
series_uf <- melt(series_uf, id.vars = 1, value.name = "valor", variable.name = "moneda")

#Otro formato para la fecha
#series_uf[, fecha := paste("01", substr(periodo, 6,7), year(ym(periodo)), sep = "-")]

#Guardar serie
write.csv2(series_uf, "Inputs/series_uf.csv", row.names = F)

#Guardar datos en base sql
dbWriteTable(con, "serie_uf", series_uf, overwrite = T)

#Desconectar la conexión a SQL
dbDisconnect(con)

#######################################
### AFILIADOS POR AFP  (EN DESUSO) ####
#######################################

# #Función para descargar datos de cotizantes del sistema de pensiones
# descargar_afiliados <- function() {
#   
#   #Crear directorio temporal para guardar el excel
#   archivo_temporal <- tempfile(fileext = ".xls") 
#   
#   #URL para la descarga
#   url <- "https://www.spensiones.cl/inf_estadistica/series_afp/afiliados/afiliados.xls"
#   
#   #Descargar archivo y guardarlo temporalmente
#   download.file(url, destfile = archivo_temporal, mode = "wb", quiet = T)
#   
#   #Leer datos
#   dt <- data.table(read_excel(path = archivo_temporal, skip = 1, sheet = 1))
#   
#   #Eliminar la columna con el total del sistema
#   dt[, SISTEMA := NULL]
#   
#   #Ordenar tabla a lo largo
#   dt <- melt(dt, id.vars = 1, value.name = "num_afiliados", variable.name = "afp", variable.factor = F)
#   
#   # Columna numero de afiliados a entero
#   dt[, num_afiliados := as.numeric(gsub("-", "0", num_afiliados))] 
#   
#   # Eliminar valores en 0
#   dt <- dt[!num_afiliados == 0]
#   
#   # Normalizar nombres de AFP
#   dt[, afp := str_to_title(ifelse(grepl("PLANVITAL", afp), "PLANVITAL", ifelse(grepl("QUALITAS", afp),
#                                                                                "QUALITAS", afp)))]
#   
#   # Renombrar variable fecha
#   setnames(dt, "FECHA", "fecha")
#   
#   
#   return(dt)
#   
# }
# descargar_afiliados <- cmpfun(descargar_afiliados)
# 
# #Descargar serie de afiliados
# afiliados <- suppressMessages(descargar_afiliados())
# 
# #Guardar datos
# write.csv2(afiliados, "Inputs/afiliados_afp.csv", row.names = F, na = "")
# 
################################################
### AFILIADOS POR TIPO DE FONDO (EN DESUSO) ####
################################################
# 
# #Función para descargar afiliados por tipo de fondo
# descargar_afiliados_fondo <- function(fechas) {
#   
#   #Lista vacía para guardar los datos
#   consolidado <- list()
#   
#   #Conexión remota a Chrome
#   remDr <- remoteDriver(remoteServerAddr = "192.168.100.34", port = 4445L, browserName = "chrome")
#   remDr$open()
#   
#   #Loop para ir fecha por fecha descargando la información
#   for (fecha in fechas) {
#     #if (fecha %in% unique(gsub(pattern = "-","",x = bd$periodo))) next
#     
#     #Auxiliar para determinar la posición en donde se guardan los datos
#     i <- match(fecha,fechas)
#     
#     #Navegar a la información de número de cotizantes por tipo de fondo
#     remDr$navigate("https://www.spensiones.cl/apps/centroEstadisticas/paginaCuadrosCCEE.php?menu=sci&menuN1=afil&menuN2=tipfon")
#     
#     tryCatch({
#       
#       #Encontrar la fecha de la descarga y hacer click
#       webelem <- remDr$findElement(using = "css", paste0("[value = 'inf_estadistica/aficot/trimestral/",
#                                                          year(ym(fecha)),
#                                                          "/",substr(fecha,5,6),"/03A']"))
#       webelem$clickElement()
#       
#       #Entrar a la información
#       webelem <- remDr$findElements(using = "css", paste0("[value = 'Ver']"))
#       webelem[[3]]$clickElement()
#       
#       #Nombres de las columnas
#       columns <- c("sexo", "< 20", "[20-25)", "[25-30)", "[30-35)", "[35-40)", "[40-45)", "[45-50)", "[50-55)",
#                    "[55-60)", "[60-65)", "[65-70)", ">=70", "sin info")
#       
#       #Función para extraer las tabla según el tipo de fondo
#       extraer_tablas <- function(fondo) {
#         
#         #Variable auxiliar para definir la tabla a extraer
#         if (fecha < 201609) {
#           a <- if (fondo == "A") {
#             3
#           } else if (fondo == "B") {
#             5
#           } else if (fondo == "C") {
#             7
#           } else if (fondo == "D") {
#             9
#           } else if (fondo == "E") {
#             11
#           } else {
#             "error"
#           }
#         } else {
#           a <- if (fondo == "A") {
#             1
#           } else if (fondo == "B") {
#             2
#           } else if (fondo == "C") {
#             3
#           } else if (fondo == "D") {
#             4
#           } else if (fondo == "E") {
#             5
#           } else {
#             "error"
#           }
#         }
#         
#         #Leer datos
#         if (fecha <= 200809) {
#           dt <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), fill = T, header = T, dec = ".")[[a]])[2:9,1:14]
#           
#         } else {
#           dt <- data.table(html_table(read_html(remDr$getPageSource()[[1]]), fill = T, header = T, dec = ".")[[a]])[2:13,1:14]
#         }
#         
#         #Asignar el nombre de las columnas
#         names(dt) <- columns
#         
#         #Ordenar la columna tramo de edad
#         dt <- melt(dt, id.vars = "sexo", variable.name = "tramo_edad", value.name = "num_afiliados")
#         
#         #Crear columna tipo de cotizante
#         dt[, tipo_afiliado := ifelse(sexo == "DEPENDIENTES", "dependientes", 
#                                      ifelse(sexo == "INDEPENDIENTES", "independientes", 
#                                             ifelse(sexo == "VOLUNTARIOS", "voluntarios", NA_character_)))]
#         
#         #Rellenar la columna tipo de cotizante
#         dt <- fill(dt, tipo_afiliado)
#         
#         #Eliminar observaciones innecesarias
#         dt <- dt[!grepl("DEPENDIENTES|VOLUNTARIOS", sexo)]
#         
#         #Cambiar de genero a sexo
#         dt[, sexo := ifelse(sexo == "Masculino", "hombres", ifelse(sexo == "Femenino", "mujeres", sexo))]
#         
#         #Crear la fecha, el fondo y pasar a dato numerico el número de cotizantes
#         dt[, ":="(tipo_fondo = fondo, fecha = paste(year(ym(fecha)), substr(fecha,5,6), sep = "-"),
#                   num_afiliados = as.numeric(gsub("\\.", "", num_afiliados)))]
#         
#         
#         return(dt)
#         
#       }
#       
#       #Fondos
#       fondos <- c("A", "B", "C", "D", "E")
#       
#       #Extraer datos de cotizantes
#       lista_afiliado <- lapply(fondos, extraer_tablas)
#       
#       #Unir las tablas de datos
#       datos_afiliado <- rbindlist(lista_afiliado)
#       
#       #Guardar datos
#       consolidado[[i]] <- datos_afiliado
#       
#     }, error = print)
#   }
#   return(rbindlist(consolidado, use.names = T))
#   
#   #Cerrar conexión remota
#   remDr$close()
#   
# }
# 
# #Descargar serie de cotizantes por fondo
# afiliados_fondo <- descargar_afiliados_fondo(grep("03$|06$|09$|12$", fechas_consulta, value = T))
# 
# #Pasar a NA los valores iguales a 0
# afiliados_fondo[, ":="(num_afiliados = ifelse(num_afiliados == 0, NA_integer_, num_afiliados))]
# 
# #Guardar datos
# write.csv2(afiliados_fondo, "Inputs/afiliados_fondo.csv", row.names = F, na = "")
