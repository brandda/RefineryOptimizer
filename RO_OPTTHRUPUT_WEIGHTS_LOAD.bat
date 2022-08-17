@ECHO OFF

if exist C:\Teradata\Current\General\RefineryOptimizer\Data\optimalthroughput.csv (
	"C:\ProgramData\Anaconda3\python.exe" "c:\teradata\current\general\refineryoptimizer\scripts\python\ro_optthruput_weights_load.py"
) else (
	echo "Input File Does Not Exist in Source Directory"
)

pause


