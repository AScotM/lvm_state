#!/usr/bin/env python3
import subprocess
import json
import sys
import os
from tabulate import tabulate

class LVMStateChecker:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.lvm_info = {}
    
    def run_command(self, cmd):
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                check=False
            )
            return result.stdout.strip(), result.returncode
        except Exception as e:
            return "", 1
    
    def display_table(self, title, headers, data, table_format="simple"):
        print("\n" + "="*80)
        print(title.center(80))
        print("="*80)
        if data:
            print(tabulate(data, headers=headers, tablefmt=table_format))
        else:
            print("No data available")
    
    def check_lvm_installation(self):
        output, code = self.run_command("lvm version 2>/dev/null | head -1")
        if code == 0 and output:
            print(f"LVM Version: {output}")
            self.lvm_info['version'] = output
            return True
        else:
            print("LVM is not installed or not in PATH")
            self.lvm_info['lvm_installed'] = False
            return False
    
    def check_physical_volumes(self):
        output, code = self.run_command("pvs --units g --nosuffix --noheadings -o pv_name,pv_size,pv_free,pv_used,vg_name,pv_attr 2>/dev/null")
        pvs_data = []
        for line in output.strip().split('\n'):
            if line.strip():
                parts = line.strip().split()
                if len(parts) >= 6:
                    size = float(parts[1]) if parts[1] else 0
                    free = float(parts[2]) if parts[2] else 0
                    used = float(parts[3]) if parts[3] else 0
                    used_percent = (used/size)*100 if size > 0 else 0
                    status = "ACTIVE" if "a" in parts[5] else "INACTIVE"
                    pvs_data.append([
                        parts[0], parts[4], f"{size:.2f} GB", 
                        f"{free:.2f} GB", f"{used_percent:.1f}%", status
                    ])
        self.display_table(
            "PHYSICAL VOLUMES (PVS)",
            ["PV Name", "VG Name", "Size", "Free", "Used %", "Status"],
            pvs_data
        )
        self.lvm_info['pvs'] = pvs_data
    
    def check_volume_groups(self):
        output, code = self.run_command("vgs --units g --nosuffix --noheadings -o vg_name,vg_size,vg_free,vg_attr,pv_count,lv_count 2>/dev/null")
        vgs_data = []
        for line in output.strip().split('\n'):
            if line.strip():
                parts = line.strip().split()
                if len(parts) >= 6:
                    size = float(parts[1]) if parts[1] else 0
                    free = float(parts[2]) if parts[2] else 0
                    free_percent = (free/size)*100 if size > 0 else 0
                    status = "ACTIVE" if "a" in parts[3] else "INACTIVE"
                    vgs_data.append([
                        parts[0], f"{size:.2f} GB", f"{free:.2f} GB",
                        f"{free_percent:.1f}%", parts[4], parts[5], status
                    ])
        self.display_table(
            "VOLUME GROUPS (VGS)",
            ["VG Name", "Size", "Free", "Free %", "PV Count", "LV Count", "Status"],
            vgs_data
        )
        self.lvm_info['vgs'] = vgs_data
    
    def check_logical_volumes(self):
        output, code = self.run_command("lvs --units g --nosuffix --noheadings -o lv_name,vg_name,lv_size,lv_attr,pool_lv,origin 2>/dev/null")
        lvs_data = []
        for line in output.strip().split('\n'):
            if line.strip():
                parts = line.strip().split()
                if len(parts) >= 4:
                    lv_type = "THIN" if "t" in parts[3] else "SNAP" if "s" in parts[3] else "NORMAL"
                    status = "ACTIVE" if "a" in parts[3] else "INACTIVE"
                    pool = parts[4] if len(parts) > 4 and parts[4] else "N/A"
                    origin = parts[5] if len(parts) > 5 and parts[5] else "N/A"
                    lvs_data.append([
                        parts[1], parts[0], f"{float(parts[2]):.2f} GB",
                        lv_type, pool, origin, status
                    ])
        self.display_table(
            "LOGICAL VOLUMES (LVS)",
            ["VG Name", "LV Name", "Size", "Type", "Pool", "Origin", "Status"],
            lvs_data
        )
        self.lvm_info['lvs'] = lvs_data
    
    def check_thin_pools(self):
        output, code = self.run_command("lvs --units g --nosuffix --noheadings -o lv_name,vg_name,data_percent,metadata_percent,thin_count 2>/dev/null | grep thin-pool")
        thin_data = []
        for line in output.strip().split('\n'):
            if line.strip():
                parts = line.strip().split()
                if len(parts) >= 5:
                    data_pct = float(parts[2]) if parts[2] else 0
                    meta_pct = float(parts[3]) if parts[3] else 0
                    thin_data.append([
                        parts[1], parts[0], f"{data_pct:.1f}%",
                        f"{meta_pct:.1f}%", parts[4]
                    ])
        self.display_table(
            "THIN POOLS",
            ["VG Name", "Pool Name", "Data Used %", "Meta Used %", "Thin Volumes"],
            thin_data
        )
        self.lvm_info['thin_pools'] = thin_data
    
    def check_lvm_mounts(self):
        output, code = self.run_command("mount | grep /dev/mapper")
        mounts_data = []
        for line in output.strip().split('\n'):
            if line:
                parts = line.split()
                if len(parts) >= 3:
                    device = parts[0]
                    mount_point = parts[2]
                    fs_type = parts[4] if len(parts) > 4 else "unknown"
                    mounts_data.append([device, mount_point, fs_type])
        self.display_table(
            "MOUNTED LVM VOLUMES",
            ["Device", "Mount Point", "Filesystem Type"],
            mounts_data
        )
        self.lvm_info['mounts'] = mounts_data
    
    def check_dm_devices(self):
        output, code = self.run_command("dmsetup status --target linear")
        dm_data = []
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 3:
                        dm_data.append([parts[0], parts[1], parts[2]])
        self.display_table(
            "DEVICE MAPPER STATUS",
            ["Device", "Start", "End/Sectors"],
            dm_data
        )
        self.lvm_info['dm_devices'] = dm_data
    
    def check_lvm_metadata_backup(self):
        backup_dirs = ['/etc/lvm/backup', '/etc/lvm/archive']
        backup_data = []
        
        for backup_dir in backup_dirs:
            try:
                if os.path.isdir(backup_dir):
                    files = [f for f in os.listdir(backup_dir) if os.path.isfile(os.path.join(backup_dir, f))]
                    backup_count = len(files)
                    backup_data.append([backup_dir, backup_count, "ACCESSIBLE"])
                    self.lvm_info[f'backup_{os.path.basename(backup_dir)}'] = backup_count
                else:
                    backup_data.append([backup_dir, 0, "NOT FOUND"])
            except PermissionError:
                backup_data.append([backup_dir, 0, "PERMISSION DENIED"])
                self.lvm_info[f'backup_{os.path.basename(backup_dir)}_accessible'] = False
        
        self.display_table(
            "LVM METADATA BACKUP STATUS",
            ["Directory", "File Count", "Access Status"],
            backup_data
        )
    
    def generate_summary(self):
        summary_data = []
        summary_data.append(["Physical Volumes", len(self.lvm_info.get('pvs', []))])
        summary_data.append(["Volume Groups", len(self.lvm_info.get('vgs', []))])
        summary_data.append(["Logical Volumes", len(self.lvm_info.get('lvs', []))])
        summary_data.append(["Thin Pools", len(self.lvm_info.get('thin_pools', []))])
        summary_data.append(["Mounted Volumes", len(self.lvm_info.get('mounts', []))])
        summary_data.append(["Device Mapper Devices", len(self.lvm_info.get('dm_devices', []))])
        
        self.display_table(
            "LVM SYSTEM SUMMARY",
            ["Component", "Count"],
            summary_data,
            "grid"
        )
        
        summary = {row[0].lower().replace(" ", "_"): row[1] for row in summary_data}
        self.lvm_info['summary'] = summary
        return summary
    
    def run_full_check(self):
        print("LVM SYSTEM STATUS CHECK")
        print("="*80)
        
        if os.geteuid() != 0:
            print("WARNING: Not running as root. Some information may be limited.")
            print("         Run with sudo for full details.\n")
        
        if not self.check_lvm_installation():
            return self.lvm_info, {}
        
        self.check_physical_volumes()
        self.check_volume_groups()
        self.check_logical_volumes()
        self.check_thin_pools()
        self.check_lvm_mounts()
        self.check_dm_devices()
        self.check_lvm_metadata_backup()
        
        summary = self.generate_summary()
        
        return self.lvm_info, summary
    
    def export_json(self, filename="lvm_state.json"):
        try:
            with open(filename, 'w') as f:
                json.dump(self.lvm_info, f, indent=2, default=str)
            print(f"LVM state exported to {filename}")
        except Exception as e:
            print(f"Failed to export JSON: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Check LVM state on Linux system')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--json', '-j', action='store_true', help='Export results to JSON')
    parser.add_argument('--output', '-o', default='lvm_state.json', help='Output JSON filename')
    
    args = parser.parse_args()
    
    checker = LVMStateChecker(verbose=args.verbose)
    lvm_info, summary = checker.run_full_check()
    
    if args.json:
        checker.export_json(args.output)
    
    issues = (
        (summary.get('physical_volumes', 0) > 0 and any("INACTIVE" in str(row) for row in lvm_info.get('pvs', []))) or
        (summary.get('volume_groups', 0) > 0 and any("INACTIVE" in str(row) for row in lvm_info.get('vgs', []))) or
        (summary.get('logical_volumes', 0) > 0 and any("INACTIVE" in str(row) for row in lvm_info.get('lvs', [])))
    )
    
    sys.exit(1 if issues else 0)


if __name__ == "__main__":
    main()
