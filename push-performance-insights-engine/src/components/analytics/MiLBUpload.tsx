import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Upload, FileText, AlertCircle } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { apiService } from "@/services/api";

interface MiLBUploadProps {
  pitcherId: string;
  pitcherName: string;
}

const MiLBUpload = ({ pitcherId, pitcherName }: MiLBUploadProps) => {
  const [file, setFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const { toast } = useToast();

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = event.target.files?.[0];
    if (selectedFile && selectedFile.type === "text/csv") {
      setFile(selectedFile);
    } else {
      toast({
        title: "Invalid File",
        description: "Please select a valid CSV file",
        variant: "destructive",
      });
    }
  };

  const handleUpload = async () => {
    if (!file) {
      toast({
        title: "No File Selected",
        description: "Please select a CSV file to upload",
        variant: "destructive",
      });
      return;
    }

    setIsUploading(true);
    try {
      await apiService.uploadMiLBData(file, pitcherId);
      toast({
        title: "Upload Successful",
        description: `MiLB data uploaded for ${pitcherName}`,
      });
      setFile(null);
      // Reset file input
      const fileInput = document.getElementById('milb-file-input') as HTMLInputElement;
      if (fileInput) fileInput.value = '';
    } catch (error) {
      toast({
        title: "Upload Failed",
        description: error instanceof Error ? error.message : "An error occurred during upload",
        variant: "destructive",
      });
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <Card className="bg-slate-800/50 border-slate-700">
      <CardHeader>
        <CardTitle className="text-white flex items-center gap-2">
          <Upload className="h-5 w-5" />
          Upload MiLB Data
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="text-sm text-slate-300">
          <p className="mb-2">Upload CSV data for <span className="font-semibold text-white">{pitcherName}</span></p>
          <p className="text-xs text-slate-400">
            Required columns: pitch_type, description, balls, strikes, events
          </p>
        </div>

        <div className="space-y-2">
          <Label htmlFor="milb-file-input" className="text-slate-300">
            Select CSV File
          </Label>
          <Input
            id="milb-file-input"
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            className="text-slate-300"
          />
        </div>

        {file && (
          <div className="flex items-center gap-2 p-3 bg-slate-700/50 rounded-lg">
            <FileText className="h-4 w-4 text-blue-400" />
            <span className="text-sm text-slate-300">{file.name}</span>
            <span className="text-xs text-slate-400">
              ({(file.size / 1024 / 1024).toFixed(2)} MB)
            </span>
          </div>
        )}

        <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-lg p-3">
          <div className="flex items-start gap-2">
            <AlertCircle className="h-4 w-4 text-yellow-500 mt-0.5" />
            <div className="text-xs text-yellow-300">
              <p className="font-semibold mb-1">CSV Format Requirements:</p>
              <ul className="space-y-1 text-yellow-200/80">
                <li>• pitch_type: Type of pitch thrown</li>
                <li>• description: Outcome of the pitch</li>
                <li>• balls: Number of balls in count</li>
                <li>• strikes: Number of strikes in count</li>
                <li>• events: Final outcome of at-bat</li>
              </ul>
            </div>
          </div>
        </div>

        <Button
          onClick={handleUpload}
          disabled={!file || isUploading}
          className="w-full bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700"
        >
          {isUploading ? "Uploading..." : "Upload MiLB Data"}
        </Button>
      </CardContent>
    </Card>
  );
};

export default MiLBUpload; 