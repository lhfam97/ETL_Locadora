using System;
using System.Collections.Generic;
using System.Diagnostics;
using Oracle.ManagedDataAccess.Client;
namespace ETL_Locadora
{
    class Program
    {
        static void Main(string[] args)
        {



            List<string> sqlInsert = new List<string>();
            //OracleConnection conn = new OracleConnection();

            using (OracleConnection conn = new OracleConnection("DATA SOURCE=localhost:1521;" + "PERSIST SECURITY INFO=True;USER ID=locadora; password=luique; Pooling = False; ")) // connect to oracle
            {
                conn.Open(); // open the oracle connection
                //////QUERY DM_SOCIO
                string sql = "select SOCIOS.COD_SOC AS ID_SOC, SOCIOS.NOM_SOC,TIPOS_SOCIOS.DSC_TPS from SOCIOS INNER JOIN TIPOS_SOCIOS ON SOCIOS.COD_TPS = TIPOS_SOCIOS.COD_TPS";
                using (OracleCommand comm = new OracleCommand(sql, conn)) // create the oracle sql command
                {
                    using (OracleDataReader rdr = comm.ExecuteReader()) // execute the oracle sql and start reading it
                    {

                        while (rdr.Read()) // loop through each row from oracle
                        {
                            sqlInsert.Add("INSERT INTO DM_SOCIO (ID_SOC, NOM_SOC,TIPO_SOCIO) values (" + rdr[0] + ",'" + rdr[1].ToString().Replace("'", "") + "','" + rdr[2].ToString().Replace("'", "") + "')");

                        }
                        rdr.Close(); // close the oracle reader
                    }
                }
                ////QUERY DM_ARTISTA
                sql = "SELECT COD_ART, TPO_ART, NAC_BRAS, NOM_ART FROM ARTISTAS";
                using (OracleCommand comm = new OracleCommand(sql, conn)) // create the oracle sql command
                {
                    using (OracleDataReader rdr = comm.ExecuteReader()) // execute the oracle sql and start reading it
                    {

                        while (rdr.Read()) // loop through each row from oracle
                        {
                            sqlInsert.Add("INSERT INTO DM_ARTISTA (ID_ART, TPO_ART,NAC_BRAS, NOM_ART) values (" + rdr[0] + ",'" + rdr[1].ToString().Replace("'", "") + "','" + rdr[2].ToString().Replace("'", "") + "','" + rdr[3].ToString().Replace("'", "") + "')");

                        }
                        rdr.Close(); // close the oracle reader
                    }
                }
                ////QUERY DM_TITULO
                sql = "SELECT COD_TIT, TPO_TIT, CLA_TIT, DSC_TIT FROM TITULOS";
                using (OracleCommand comm = new OracleCommand(sql, conn)) // create the oracle sql command
                {
                    using (OracleDataReader rdr = comm.ExecuteReader()) // execute the oracle sql and start reading it
                    {

                        while (rdr.Read()) // loop through each row from oracle
                        {
                            sqlInsert.Add("INSERT INTO DM_TITULO (ID_TITULO, TPO_TITULO,CLA_TITULO, DSC_TITULO) values (" + rdr[0] + ",'" + rdr[1].ToString().Replace("'", "") + "','" + rdr[2].ToString().Replace("'", "") + "','" + rdr[3].ToString().Replace("'", "") + "')");

                        }
                        rdr.Close(); // close the oracle reader
                    }
                }
                ////QUERY DM_GRAVADORA
                sql = "SELECT COD_GRAV, UF_GRAV, NAC_BRAS, NOM_GRAV FROM GRAVADORAS";
                using (OracleCommand comm = new OracleCommand(sql, conn)) // create the oracle sql command
                {
                    using (OracleDataReader rdr = comm.ExecuteReader()) // execute the oracle sql and start reading it
                    {

                        while (rdr.Read()) // loop through each row from oracle
                        {
                            sqlInsert.Add("INSERT INTO DM_GRAVADORA (ID_GRAV, UF_GRAV, NAC_BRAS, NOM_GRAV) values (" + rdr[0] + ",'" + rdr[1].ToString().Replace("'", "") + "','" + rdr[2].ToString().Replace("'", "") + "','" + rdr[3].ToString().Replace("'", "") + "')");

                        }
                        rdr.Close(); // close the oracle reader
                    }
                }
                //QUERY TEMPO
                sql = "SELECT DISTINCT DAT_DEV FROM ITENS_LOCACOES WHERE DAT_DEV IS NOT NULL";
                using (OracleCommand comm = new OracleCommand(sql, conn)) // create the oracle sql command
                {
                    using (OracleDataReader rdr = comm.ExecuteReader()) // execute the oracle sql and start reading it
                    {
                        DateTime data = DateTime.Now;
                        int ids = 1;
                        string turno = "";
                        while (rdr.Read()) // loop through each row from oracle
                        {
                            data = Convert.ToDateTime(rdr[0].ToString());

                            for (int i = 7; i < 22; i++)
                            {
                                if (i < 12)
                                {
                                    turno = "Manha";
                                }
                                else if (i < 18)
                                {
                                    turno = "Tarde";
                                }
                                else
                                {
                                    turno = "Noite";
                                }
                                int anomes = Int32.Parse(data.Year.ToString()) + Int32.Parse(data.Month.ToString());

                                sqlInsert.Add("INSERT INTO DM_TEMPO VALUES(" + ids + "," + data.Year + "," + data.Month + "," + anomes + ",'"
                                + data.Month.ToString() + "','" + data.Month.ToString() + data.Year + "','"
                                + data.Month.ToString() + "'," + data.Day + ", TO_DATE('" + data.ToString("yyyy/MM/dd") + "', 'YYYY/MM/DD')" + "," + i + ",'" + turno + "')");
                                ids++;
                            }


                        }
                        rdr.Close(); // close the oracle reader
                    }
                }
                //QUERY TABELA_FATOS

                sql = "SELECT SOCIOS.COD_SOC, TITULOS.COD_TIT, ARTISTAS.COD_ART, GRAVADORAS.COD_GRAV, ITENS_LOCACOES.VAL_LOC, ITENS_LOCACOES.DAT_LOC, ITENS_LOCACOES.DAT_PREV, ITENS_LOCACOES.DAT_DEV"
                    + " FROM LOCACOES INNER JOIN SOCIOS on LOCACOES.COD_SOC = SOCIOS.COD_SOC"
                    + " INNER JOIN ITENS_LOCACOES on (ITENS_LOCACOES.COD_SOC = LOCACOES.COD_SOC AND ITENS_LOCACOES.DAT_LOC = LOCACOES.DAT_LOC)"
                    + " INNER JOIN TITULOS on ITENS_LOCACOES.COD_TIT = TITULOS.COD_TIT"
                    + " INNER JOIN ARTISTAS on TITULOS.COD_ART = ARTISTAS.COD_ART"
                    + " INNER JOIN GRAVADORAS on ARTISTAS.COD_GRAV = GRAVADORAS.COD_GRAV"
                    + " WHERE ITENS_LOCACOES.DAT_DEV IS NOT NULL AND (ITENS_LOCACOES.STA_MUL = 'Q' OR ITENS_LOCACOES.STA_MUL = 'N')";
                using (OracleCommand comm = new OracleCommand(sql, conn)) // create the oracle sql command
                {
                    using (OracleDataReader rdr = comm.ExecuteReader()) // execute the oracle sql and start reading it
                    {
                        double multa;
                        double tempoDev;

                        while (rdr.Read()) // loop through each row from oracle
                        {
                           
                            DateTime dataLoc = Convert.ToDateTime(rdr[5].ToString());
                            DateTime dataPrev = Convert.ToDateTime(rdr[6].ToString());
                            DateTime dataDev = Convert.ToDateTime(rdr[7].ToString());
                       
                            tempoDev = (dataDev - dataPrev).TotalDays;
                    
                            if(tempoDev > 0)
                            {
                                multa = (Double.Parse(rdr[4].ToString()) + (tempoDev - 1) * 0.3 * Double.Parse(rdr[4].ToString()));
                                multa = Double.Parse(multa.ToString().Replace(",", "."));
                            }
                            else
                            {
                                multa = 0;
                            }
                            sqlInsert.Add("insert into ft_locacoes values(" + rdr[0] + "," + rdr[1] + "," + rdr[2] + ","
                        + rdr[3] + "," + "(select id_tempo from dm_tempo where TO_DATE('" + dataDev.ToString("yyyy/MM/dd")
                        + "', 'YYYY/MM/DD') = dm_tempo.dt_tempo AND rownum = 1)," + rdr[4] + "," + (tempoDev)
                        + "," + (multa) + ")");
                            Debug.WriteLine("RDR4" + tempoDev + "\n");
                            Debug.WriteLine("TEMPODEV" + rdr[4] + "\n");
                            Debug.WriteLine("MULTA" + multa + "\n");
                        }
                       
                        rdr.Close(); // close the oracle reader
                    }
                }
                conn.Close(); // close the oracle connection
            }
            OracleConnection conndw = new OracleConnection("DATA SOURCE=localhost:1521;" + "PERSIST SECURITY INFO=True;USER ID=dw_locadora; password=luique; Pooling = False; ");
            conndw.Open();
            foreach (string insert in sqlInsert)
            {
                try
                {
                    Debug.WriteLine(insert);
                    OracleCommand commdw = new OracleCommand(insert, conndw);
                    commdw.ExecuteReader();
                }

                catch (Exception ex)
                {
                    Debug.WriteLine(ex.ToString());
                }
            }
            conndw.Close();

        }

    }


}

