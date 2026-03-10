using System.Diagnostics;
using System.Windows.Forms;

var exeDir = AppContext.BaseDirectory;
var startBat = Path.Combine(exeDir, "START_LABEL_ENRICHER.bat");

if (!File.Exists(startBat))
{
    MessageBox.Show(
        $"Missing startup script:\n{startBat}\n\nPut this launcher next to START_LABEL_ENRICHER.bat.",
        "Label Enricher",
        MessageBoxButtons.OK,
        MessageBoxIcon.Error
    );
    return;
}

try
{
    Process.Start(new ProcessStartInfo
    {
        FileName = startBat,
        WorkingDirectory = exeDir,
        UseShellExecute = true,
    });
}
catch (Exception ex)
{
    MessageBox.Show(
        $"Failed to launch startup script.\n\n{ex.Message}",
        "Label Enricher",
        MessageBoxButtons.OK,
        MessageBoxIcon.Error
    );
}
