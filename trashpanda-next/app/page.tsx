import { Topbar } from "@/components/Topbar";
import { UploadDropzone } from "@/components/UploadDropzone";
import { RecentJobs } from "@/components/RecentJobs";

export default function ConsolePage() {
  return (
    <>
      <div className="fade-up">
        <Topbar
          breadcrumb={["WORKSPACE", "CONSOLE"]}
          title="DATA/PURGE CONSOLE"
          titleSlice="/"
          meta={[
            { label: "ENGINE", value: "ONLINE", accent: true },
            { label: "READY FOR", value: "CSV · XLSX" },
          ]}
        />
      </div>
      <div className="fade-up">
        <UploadDropzone />
      </div>
      <div className="fade-up">
        <RecentJobs />
      </div>
    </>
  );
}
