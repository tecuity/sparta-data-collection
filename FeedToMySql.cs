﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Geotab.Checkmate.ObjectModel;
using Geotab.Checkmate.ObjectModel.Engine;
using Geotab.Checkmate.ObjectModel.Exceptions;
using Exception = System.Exception;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace sync_geotab
{
    class FeedToMySql
    {
        /// <summary>
        /// The exception event header
        /// </summary>
        public const string ExceptionEventHeader = "sId,sVehicle Name,sVehicle Serial Number,sVIN,sDiagnostic Name,iDiagnostic Code,sSource Name,sDriver Name,sDriver Keys,sRule Name,sActive From,sActive To";

        /// <summary>
        /// The file prefix for exception data
        /// </summary>
        public const string ExceptionEventPrefix = "Exception_Events";

        /// <summary>
        /// The fault data header
        /// </summary>
        public const string FaultDataHeader = "sVehicle Name,sVehicle Serial Number,sVIN,sDate,sDiagnostic Name,sFailure Mode Name,iFailure Mode Code,sFailure Mode Source,sController Name,iCount,sActive,bMalfunction Lamp,bRed Stop Lamp,bAmber Warning Lamp,bProtect Lamp,sDismiss Date,sDismiss User";

        /// <summary>
        /// The file prefix for faults
        /// </summary>
        public const string FaultPrefix = "Fault_Data";

        /// <summary>
        /// The GPS data header
        /// </summary>
        public const string GpsDataHeader = "sVehicle Name,sVehicle Serial Number,sVIN,sDate,dLongitude,dLatitude,iSpeed";

        /// <summary>
        /// The file prefix for gps data
        /// </summary>
        public const string GpsPrefix = "Gps_Data";

        /// <summary>
        /// The status data header
        /// </summary>
        public const string StatusDataHeader = "sVehicle Name,sVehicle Serial Number,sVIN,sDate,sDiagnostic Name,iDiagnostic Code,sSource Name,dValue,sUnits";

        /// <summary>
        /// The file prefix for status data
        /// </summary>
        public const string StatusPrefix = "Status_Data";

        /// <summary>
        /// The trip header
        /// </summary>
        public const string TripHeader = "sVehicleName,sVehicleSerialNumber,sVin,sDriver Name,sDriver Keys,sTrip Start Time,sTrip End Time,dTrip Distance";

        /// <summary>
        /// The file prefix for trips
        /// </summary>
        public const string TripPrefix = "Trips";

        readonly IList<ExceptionEvent> exceptionEvents;
        readonly IList<FaultData> faultRecords;
        readonly IList<LogRecord> gpsRecords;
        readonly string path;
        readonly IList<StatusData> statusRecords;
        readonly IList<Trip> trips;

        /// <summary>
        /// Initializes a new instance of the <see cref="FeedToMySql" /> class.
        /// </summary>
        /// <param name="gpsRecords">The GPS records.</param>
        /// <param name="statusRecords">The status records.</param>
        /// <param name="faultRecords">The fault records.</param>
        /// <param name="trips">The trips.</param>
        /// <param name="exceptionEvents">The exception events.</param>
        public FeedToMySql(IList<LogRecord> gpsRecords = null, IList<StatusData> statusRecords = null, IList<FaultData> faultRecords = null, IList<Trip> trips = null, IList<ExceptionEvent> exceptionEvents = null)
        {
            this.gpsRecords = gpsRecords ?? new List<LogRecord>();
            this.statusRecords = statusRecords ?? new List<StatusData>();
            this.faultRecords = faultRecords ?? new List<FaultData>();
            this.trips = trips ?? new List<Trip>();
            this.exceptionEvents = exceptionEvents ?? new List<ExceptionEvent>();
        }

        /// <summary>
        /// Runs the instance.
        /// </summary>
        public void Run()
        {
            if (gpsRecords.Count > 0)
            {
                WriteDataToMySql<LogRecord>();
            }
            if (statusRecords.Count > 0)
            {
                WriteDataToMySql<StatusData>();
            }
            if (faultRecords.Count > 0)
            {
                WriteDataToMySql<FaultData>();
            }
            if (trips.Count > 0)
            {
                WriteDataToMySql<Trip>();
            }
            if (exceptionEvents.Count > 0)
            {
                WriteDataToMySql<ExceptionEvent>();
            }
        }
        /*
        static void AppendDeviceValues(StringBuilder sb, Device device)
        {
            AppendValues(sb, device.Name.Replace(",", " "));
            AppendValues(sb, device.SerialNumber);
            GoDevice goDevice = device as GoDevice;
            AppendValues(sb, (goDevice == null ? "" : goDevice.VehicleIdentificationNumber ?? "").Replace(",", " "));
        }

        static void AppendDiagnosticValues(StringBuilder sb, Diagnostic diagnostic)
        {
            AppendName(sb, diagnostic);
            AppendValues(sb, diagnostic.Code);
            Source source = diagnostic.Source;
            if (source != null)
            {
                AppendName(sb, source);
            }
            else
            {
                AppendValues(sb, "");
            }
        }

        static void AppendDriverValues(StringBuilder sb, Driver driver)
        {
            AppendName(sb, driver);
            List<Key> keys = driver.Keys;
            if (keys != null)
            {
                for (int i = 0; i < keys.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append('~');
                    }
                    sb.Append(keys[i].SerialNumber);
                }
            }
            sb.Append(',');
        }

        static void AppendName(StringBuilder sb, NameEntity entity)
        {
            AppendValues(sb, entity.IsSystemEntity() ? entity.GetType().ToString().Replace("Geotab.Checkmate.ObjectModel.", "").Replace(",", " ") : entity.Name.Replace(",", " "));
        }

        static void AppendValues(StringBuilder sb, object o)
        {
            sb.Append(o);
            sb.Append(',');
        }

        //static string MakeFileName(string prefix) => prefix + "-" + DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss") + ".csv";
        static string MakeFileName(string prefix) => prefix + ".csv";

        static void Write<T>(TextWriter writer, T entity, Action<StringBuilder, T> action)
        {
            StringBuilder sb = new StringBuilder();
            action(sb, entity);
            writer.WriteLine(sb.ToString().TrimEnd(','));
        }
        */
        void WriteDataToMySql<T>()
            where T : class
        {
            try
            {
                
                string connStr = "server=localhost;user=root;database=spartan;password=LetMeInPlease123!";
                MySqlConnection conn = new MySqlConnection(connStr);
        
                Console.WriteLine("Connecting to MySql...");
                conn.Open();
                Console.WriteLine("Writing to MySql...");
                Type type = typeof(T);
                if (type == typeof(ExceptionEvent))
                {
                    foreach (ExceptionEvent exceptionEvent in exceptionEvents)
                    {
                        string sql = "";
                        try
                        {
                            sql = "INSERT INTO exception_events (geotab_id, vehicle, vehicle_serial, vin, diagnostic_name, diagnostic_code, source_name, driver_name, rule_name, distance, active_from, active_to, version) ";
                            sql += " VALUES(";
                            sql += "'" + exceptionEvent.Id + "',";
                            sql += "'" + exceptionEvent.Device.Name.Replace(",", " ") + "',";
                            sql += "'" + exceptionEvent.Device.SerialNumber + "',";
                            GoDevice goDevice = exceptionEvent.Device as GoDevice;
                            if (goDevice != null)
                            {
                                sql += "'" + (goDevice.VehicleIdentificationNumber ?? "").Replace(",", " ") + "',";
                            } else
                            {
                                sql += "'',";
                            }
                            

                            sql += "'" + exceptionEvent.Diagnostic.Name.Replace(",", " ") + "',";
                            sql += "'" + exceptionEvent.Diagnostic.Code + "',";

                            string sourceVal = (exceptionEvent.Diagnostic.Source != null) ? exceptionEvent.Diagnostic.Source.Name : "";
                            sql += "'" + sourceVal + "',";
                            sql += "'" + exceptionEvent.Driver.Name + "',";
                            sql += "'" + exceptionEvent.Rule.Name + "',";
                            sql += "" + exceptionEvent.Distance + ",";
                            sql += "str_to_date(\"" + exceptionEvent.ActiveFrom + "\", \"%m/%d/%Y %h:%i:%s %p\"),";
                            sql += "str_to_date(\"" + exceptionEvent.ActiveTo + "\", \"%m/%d/%Y %h:%i:%s %p\"),";

                            sql += exceptionEvent.Version;

                            sql += ");";



                            MySqlCommand cmd = new MySqlCommand(sql, conn);
                            cmd.ExecuteNonQuery();
                        } catch (Exception ex)
                        {
                            Console.WriteLine(sql);
                            Console.WriteLine(ex.ToString());
                        }
                    }
                }

                conn.Close();
            } catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            } finally
            {
                Console.WriteLine("Done");
            }



            /*
            try
            {
                Type type = typeof(T);
                if (type == typeof(LogRecord))
                {
                    using (TextWriter writer = new StreamWriter(Path.Combine(path, MakeFileName(GpsPrefix)), true))
                    {
                        writer.WriteLine(GpsDataHeader);
                        foreach (LogRecord gpsRecord in gpsRecords)
                        {
                            Write(writer, gpsRecord, (StringBuilder sb, LogRecord logRecord) =>
                            {
                                AppendDeviceValues(sb, logRecord.Device);
                                AppendValues(sb, logRecord.DateTime);
                                AppendValues(sb, logRecord.Longitude);
                                AppendValues(sb, logRecord.Latitude);
                                AppendValues(sb, logRecord.Speed);
                            });
                        }
                    }
                }
                else if (type == typeof(StatusData))
                {
                    using (TextWriter writer = new StreamWriter(Path.Combine(path, MakeFileName(StatusPrefix)), true))
                    {
                        writer.WriteLine(StatusDataHeader);
                        foreach (StatusData statusRecord in statusRecords)
                        {
                            Write(writer, statusRecord, (StringBuilder sb, StatusData statusData) =>
                            {
                                AppendDeviceValues(sb, statusData.Device);
                                AppendValues(sb, statusData.DateTime);
                                Diagnostic diagnostic = statusData.Diagnostic;
                                AppendDiagnosticValues(sb, diagnostic);
                                AppendValues(sb, statusData.Data);
                                if (diagnostic is DataDiagnostic dataDiagnostic)
                                {
                                    AppendName(sb, dataDiagnostic.UnitOfMeasure);
                                }
                            });
                        }
                    }
                }
                else if (type == typeof(FaultData))
                {
                    using (TextWriter writer = new StreamWriter(Path.Combine(path, MakeFileName(FaultPrefix)), true))
                    {
                        writer.WriteLine(FaultDataHeader);
                        foreach (FaultData faultRecord in faultRecords)
                        {
                            Write(writer, faultRecord, (StringBuilder sb, FaultData faultData) =>
                            {
                                AppendDeviceValues(sb, faultData.Device);
                                AppendValues(sb, faultData.DateTime);
                                AppendName(sb, faultData.Diagnostic);
                                FailureMode failureMode = faultData.FailureMode;
                                AppendName(sb, failureMode);
                                AppendValues(sb, failureMode.Code);
                                if (failureMode is NoFailureMode)
                                {
                                    AppendValues(sb, "None");
                                }
                                else
                                {
                                    AppendName(sb, failureMode.Source);
                                }
                                AppendName(sb, faultData.Controller);
                                AppendValues(sb, faultData.Count);
                                AppendValues(sb, faultData.FaultState);
                                AppendValues(sb, faultData.MalfunctionLamp);
                                AppendValues(sb, faultData.RedStopLamp);
                                AppendValues(sb, faultData.AmberWarningLamp);
                                AppendValues(sb, faultData.ProtectWarningLamp);
                                AppendValues(sb, faultData.DismissDateTime);
                                User dismissUser = faultData.DismissUser;
                                if (dismissUser != null)
                                {
                                    AppendValues(sb, faultData.DismissUser.Name.Replace(",", " "));
                                }
                            });
                        }
                    }
                }
                else if (type == typeof(Trip))
                {
                    using (TextWriter writer = new StreamWriter(Path.Combine(path, MakeFileName(TripPrefix)), true))
                    {
                        writer.WriteLine(TripHeader);
                        foreach (Trip tripToWrite in trips)
                        {
                            Write(writer, tripToWrite, (StringBuilder sb, Trip trip) =>
                            {
                                AppendDeviceValues(sb, trip.Device);
                                AppendDriverValues(sb, trip.Driver);
                                AppendValues(sb, trip.Start);
                                AppendValues(sb, trip.Stop);
                                AppendValues(sb, trip.Distance);
                            });
                        }
                    }
                }
                else if (type == typeof(ExceptionEvent))
                {
                    using (TextWriter writer = new StreamWriter(Path.Combine(path, MakeFileName(ExceptionEventPrefix)), true))
                    {
                        writer.WriteLine(ExceptionEventHeader);
                        foreach (ExceptionEvent exceptionEventToWrite in exceptionEvents)
                        {
                            Write(writer, exceptionEventToWrite, (StringBuilder sb, ExceptionEvent exceptionEvent) =>
                            {
                                AppendValues(sb, exceptionEvent.Id);
                                AppendDeviceValues(sb, exceptionEvent.Device);
                                AppendDiagnosticValues(sb, exceptionEvent.Diagnostic);
                                AppendDriverValues(sb, exceptionEvent.Driver);
                                AppendName(sb, exceptionEvent.Rule);
                                AppendValues(sb, exceptionEvent.ActiveFrom);
                                AppendValues(sb, exceptionEvent.ActiveTo);
                                AppendValues(sb, exceptionEvent.Version);
                            });
                        }
                    }
                }
                else
                {
                    throw new NotSupportedException(type.ToString());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                if (e is IOException)
                {
                    // Possiable system out of memory exception or file lock. Log then sleep for a minute and continue.
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                }
            }
            */
        }
    }
}