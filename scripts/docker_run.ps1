param(
    [Parameter(Mandatory = $true)] [string] $DataDir,
    [Parameter(Mandatory = $true)] [string] $OutputDir,
    [ValidateRange(1, 4096)] [int] $Cpus = 4,
    [ValidateRange(1, 1024)] [int] $MemoryGb = 8,
    [string] $Image = 'shopping-sessions:rust'
)

$ErrorActionPreference = 'Stop'
$data = (Resolve-Path -LiteralPath $DataDir).Path
$output = (New-Item -ItemType Directory -Force -Path $OutputDir).FullName
$scratch = (New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot '../.buildathon/scratch')).FullName
$memory = "${MemoryGb}g"

$dockerArgs = @(
    'run', '--rm', '--platform', 'linux/amd64',
    '--network', 'none', '--read-only', '--tmpfs', '/tmp:rw,size=64m,mode=1777',
    '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
    '--pids-limit', '4096', '--cpus', "$Cpus", '--memory', $memory, '--memory-swap', $memory,
    '--user', '1000:1000',
    '--mount', "type=bind,source=$data,target=/data,readonly",
    '--mount', "type=bind,source=$output,target=/output",
    '--mount', "type=bind,source=$scratch,target=/scratch",
    '--env', "NCPU=$Cpus", '--env', "MEM_GB=$MemoryGb", '--env', 'PROFILE',
    $Image
)
& docker @dockerArgs
exit $LASTEXITCODE
