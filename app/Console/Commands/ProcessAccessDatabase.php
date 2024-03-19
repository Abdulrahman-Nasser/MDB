<?php

namespace App\Console\Commands;


use Illuminate\Console\Command;
use Illuminate\Support\Facades\File;

class ProcessAccessDatabase extends Command
{

    protected $signature = 'accessdb:process {file} {columns?}';
    protected $description = 'Process an MDB or ACCDB file';

    public function handle()
    {
        date_default_timezone_set('Egypt');

        $filePath = $this->argument('file');
        $columns = $this->argument('columns') ? explode(',', $this->argument('columns')) : ['*'];

        $db = odbc_connect("DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=$filePath", '', '');

        if ($db) {
            $tables = odbc_tables($db);

            File::ensureDirectoryExists(public_path('All_Tables'));
            File::ensureDirectoryExists(public_path('Specified_Columns'));

            while ($table = odbc_fetch_object($tables)) {
                $tableName = $table->TABLE_NAME;
                $this->info("Table Name: $tableName");

                $result = @odbc_exec($db, "SELECT * FROM $tableName");

                if ($result) {
                    $data = [];

                    if ($columns[0] === '*') {
                        while ($row = odbc_fetch_array($result)) {
                            $data[] = $row;
                        }
                        $jsonFilePathAll = public_path('All_Tables/' . $tableName . '.json');
                        file_put_contents($jsonFilePathAll, json_encode($data));
                    } else {
                        $specifiedData = [];
                        while ($row = odbc_fetch_array($result)) {
                            $data = [];
                            foreach ($columns as $column) {
                                $data[$column] = $row[$column] ?? null;
                            }
                            $specifiedData[] = $data;
                        }
                        $jsonFilePathSpecified = public_path('Specified_Columns/' . $tableName . '.json');
                        file_put_contents($jsonFilePathSpecified, json_encode($specifiedData));
                    }

                    if ($tableName === 'dt_LCMS_Patch_Processed') {
                        $resultDtLCMS = odbc_exec($db, "SELECT AREA, SEVERITY FROM $tableName");

                        $dataDtLCMS = [];
                        while ($rowDtLCMS = odbc_fetch_array($resultDtLCMS)) {
                            $dataDtLCMS[] = $rowDtLCMS;
                        }

                        $dateTime = date('Y-m-d_H-i-s');
                        $jsonFileNameDtLCMS = 'dt_LCMS_Patch_Processed_' . $dateTime . '.json';
                        $jsonFilePathDtLCMS = public_path('dt_LCMS/' . $jsonFileNameDtLCMS);

                        $fileIndex = 1;
                        while (File::exists($jsonFilePathDtLCMS)) {
                            $jsonFileNameDtLCMS = 'dt_LCMS_Patch_Processed_' . $dateTime . '_' . $fileIndex . '.json';
                            $jsonFilePathDtLCMS = public_path('dt_LCMS/' . $jsonFileNameDtLCMS);
                            $fileIndex++;
                        }

                        File::ensureDirectoryExists(public_path('dt_LCMS'));
                        file_put_contents($jsonFilePathDtLCMS, json_encode($dataDtLCMS));

                        $this->info("JSON data for $tableName has been saved to $jsonFileNameDtLCMS in dt_LCMS directory.");
                    }
                } else {
                    $this->error("Error querying table: $tableName");
                }
            }

            odbc_close($db);
        } else {
            $this->error('Error connecting to the database.');
        }
    }

}
