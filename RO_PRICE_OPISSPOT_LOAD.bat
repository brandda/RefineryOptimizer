@ECHO OFF

if exist C:\Teradata\Current\General\RefineryOptimizer\Data\pricing\opis\opisspotsdatafiles.csv (
	"C:\ProgramData\Anaconda3\python.exe" "c:\teradata\current\general\refineryoptimizer\scripts\python\ro_price_opisspot_load.py"
) else (
	echo "Input File Does Not Exist in Source Directory"
)

pause


