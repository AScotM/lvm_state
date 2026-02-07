#!/usr/bin/env python3

import subprocess
import json
import sys
import os
import shlex
import re
import pathlib
import time
import textwrap
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict
import math
import signal

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False
    print("Warning: 'tabulate' not installed. Using simple table formatting.", file=sys.stderr)


class Color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'


class LVMStatus(Enum):
    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"


@dataclass
class PhysicalVolume:
    name: str
    vg_name: str
    size_gb: float
    free_gb: float
    used_percent: float
    status: str
    attributes: str
    dev_size: Optional[str] = None
    uuid: Optional[str] = None
    disk_errors: Optional[int] = None
    disk_model: Optional[str] = None
    
    @property
    def lvm_status(self) -> LVMStatus:
        if "unknown" in self.status.lower() or "missing" in self.status.lower():
            return LVMStatus.CRITICAL
        if "inactive" in self.status.lower():
            return LVMStatus.WARNING
        if self.disk_errors and self.disk_errors > 0:
            return LVMStatus.CRITICAL
        return LVMStatus.HEALTHY


@dataclass
class VolumeGroup:
    name: str
    size_gb: float
    free_gb: float
    free_percent: float
    pv_count: int
    lv_count: int
    attributes: str
    lock_type: Optional[str] = None
    lock_args: Optional[str] = None
    uuid: Optional[str] = None
    extent_size: Optional[str] = None
    
    @property
    def lvm_status(self) -> LVMStatus:
        if "p" in self.attributes:
            return LVMStatus.CRITICAL
        if "x" in self.attributes:
            return LVMStatus.CRITICAL
        if self.lock_type and self.lock_type not in ["normal", "None", ""]:
            return LVMStatus.WARNING
        if self.free_percent < 5:
            return LVMStatus.CRITICAL
        if self.free_percent < 10:
            return LVMStatus.WARNING
        if "p" not in self.attributes and "x" not in self.attributes:
            return LVMStatus.HEALTHY
        return LVMStatus.WARNING


@dataclass
class LogicalVolume:
    name: str
    vg_name: str
    size_gb: float
    lv_type: str
    pool: Optional[str]
    origin: Optional[str]
    status: str
    attributes: str
    raid_sync_percent: Optional[float] = None
    cache_total_blocks: Optional[int] = None
    cache_used_blocks: Optional[int] = None
    uuid: Optional[str] = None
    segments: Optional[str] = None
    
    @property
    def lvm_status(self) -> LVMStatus:
        if "s" in self.attributes:
            snapshot_status = self._check_snapshot_status()
            if snapshot_status != LVMStatus.HEALTHY:
                return snapshot_status
        
        if "a" not in self.attributes:
            return LVMStatus.CRITICAL
        if "m" in self.attributes:
            return LVMStatus.WARNING
        if "r" in self.attributes and self.raid_sync_percent is not None:
            if self.raid_sync_percent < 100:
                return LVMStatus.WARNING
        if "C" in self.attributes and self.cache_used_blocks is not None and self.cache_total_blocks is not None:
            cache_usage = (self.cache_used_blocks / self.cache_total_blocks * 100) if self.cache_total_blocks > 0 else 0
            if cache_usage > 90:
                return LVMStatus.WARNING
        return LVMStatus.HEALTHY
    
    def _check_snapshot_status(self) -> LVMStatus:
        if "O" in self.attributes:
            return LVMStatus.WARNING
        return LVMStatus.HEALTHY


@dataclass
class ThinPool:
    name: str
    vg_name: str
    data_percent: float
    metadata_percent: float
    thin_count: int
    lv_uuid: Optional[str] = None
    metadata_size_gb: Optional[float] = None
    data_size_gb: Optional[float] = None
    
    @property
    def lvm_status(self) -> LVMStatus:
        if self.data_percent > 95:
            return LVMStatus.CRITICAL
        if self.data_percent > 85:
            return LVMStatus.WARNING
        if self.metadata_percent > 95:
            return LVMStatus.CRITICAL
        if self.metadata_percent > 85:
            return LVMStatus.WARNING
        return LVMStatus.HEALTHY


@dataclass
class CachePool:
    name: str
    vg_name: str
    cache_total_blocks: int
    cache_used_blocks: int
    cache_dirty_blocks: Optional[int] = None
    lv_uuid: Optional[str] = None
    
    @property
    def lvm_status(self) -> LVMStatus:
        if self.cache_total_blocks == 0:
            return LVMStatus.UNKNOWN
        usage_percent = (self.cache_used_blocks / self.cache_total_blocks) * 100
        if usage_percent > 95:
            return LVMStatus.CRITICAL
        if usage_percent > 85:
            return LVMStatus.WARNING
        return LVMStatus.HEALTHY


@dataclass
class DiskInfo:
    name: str
    model: Optional[str] = None
    size_gb: Optional[float] = None
    read_errors: int = 0
    write_errors: int = 0
    smart_status: Optional[str] = None


@dataclass
class LVMHealthCheck:
    pvs: List[PhysicalVolume]
    vgs: List[VolumeGroup]
    lvs: List[LogicalVolume]
    thin_pools: List[ThinPool]
    cache_pools: List[CachePool]
    disks: List[DiskInfo]
    mounts: List[Dict[str, str]]
    dm_devices: List[Dict[str, str]]
    metadata_backup: Dict[str, Any]
    timestamp: float
    issues: List[str]
    warnings: List[str]
    
    @property
    def overall_status(self) -> LVMStatus:
        critical_components = []
        for pv in self.pvs:
            if pv.lvm_status == LVMStatus.CRITICAL:
                critical_components.append(f"PV:{pv.name}")
        
        for vg in self.vgs:
            if vg.lvm_status == LVMStatus.CRITICAL:
                critical_components.append(f"VG:{vg.name}")
        
        for lv in self.lvs:
            if lv.lvm_status == LVMStatus.CRITICAL:
                critical_components.append(f"LV:{lv.vg_name}/{lv.name}")
        
        for pool in self.thin_pools:
            if pool.lvm_status == LVMStatus.CRITICAL:
                critical_components.append(f"Pool:{pool.vg_name}/{pool.name}")
        
        for pool in self.cache_pools:
            if pool.lvm_status == LVMStatus.CRITICAL:
                critical_components.append(f"Cache:{pool.vg_name}/{pool.name}")
        
        for disk in self.disks:
            if disk.read_errors > 10 or disk.write_errors > 10:
                critical_components.append(f"Disk:{disk.name}")
        
        if critical_components:
            return LVMStatus.CRITICAL
        if self.warnings:
            return LVMStatus.WARNING
        return LVMStatus.HEALTHY


class LVMStateChecker:
    def __init__(self, verbose=False, color=True, timeout=30):
        self.verbose = verbose
        self.use_color = color and sys.stdout.isatty()
        self.timeout = timeout
        self.health_check = None
        self._is_root = os.geteuid() == 0
        self._cache = {}
        self._cache_timestamp = 0
        self._cache_ttl = 300
        
    def _colorize(self, text: str, color: str) -> str:
        if self.use_color:
            return f"{color}{text}{Color.RESET}"
        return text
    
    def _sanitize_lvm_name(self, name: str) -> str:
        if not name:
            return ""
        import re
        name = re.sub(r'[\x00-\x1F\x7F]', '', name)
        name = name.replace('\n', '').replace('\r', '')
        return name.strip()
    
    def _validate_command(self, cmd_args: List[str]) -> bool:
        if not cmd_args:
            return False
        
        cmd_path = cmd_args[0]
        if not os.path.exists(cmd_path) and '/' not in cmd_path:
            try:
                subprocess.run(["which", cmd_path], capture_output=True, check=False)
            except:
                return False
        return True
    
    def _run_command(self, cmd_args: List[str]) -> Tuple[str, int]:
        if not self._validate_command(cmd_args):
            return f"Invalid command: {cmd_args[0] if cmd_args else 'None'}", 1
        
        cache_key = "|".join(cmd_args)
        if cache_key in self._cache:
            cached_time, cached_result = self._cache[cache_key]
            if time.time() - cached_time < self._cache_ttl:
                return cached_result
        
        try:
            result = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
                start_new_session=True
            )
            output = result.stdout.strip()
            self._cache[cache_key] = (time.time(), output)
            return output, result.returncode
        except subprocess.TimeoutExpired as e:
            try:
                if hasattr(e, 'cmd') and hasattr(e, 'timeout'):
                    os.killpg(os.getpgid(e.pid), signal.SIGKILL)
            except:
                pass
            return f"Command timed out after {self.timeout}s", 124
        except Exception as e:
            return f"Command failed: {e}", 1
    
    def _parse_lvm_attributes(self, attr_string: str) -> Dict[str, bool]:
        attr_map = {
            'a': 'active',
            's': 'snapshot',
            'm': 'mirrored',
            'M': 'mirror_log',
            'o': 'origin',
            'O': 'origin_with_merging_snapshot',
            'r': 'raid',
            'R': 'raid_member',
            'c': 'cache',
            'C': 'cache_pool',
            'V': 'virtual',
            'i': 'mirror_image',
            'I': 'mirror_image_out_of_sync',
            'l': 'log_device',
            'p': 'pvmove',
            'v': 'virtual_volume',
            'e': 'exclusive',
            'd': 'device_mapper_error',
        }
        result = {}
        for i, (char, desc) in enumerate(attr_map.items()):
            if i < len(attr_string):
                result[desc] = attr_string[i] == char
        return result
    
    def _safe_float(self, value: str, default: float = 0.0) -> float:
        try:
            clean_value = value.replace(',', '.').strip()
            if clean_value:
                return float(clean_value)
        except (ValueError, AttributeError):
            pass
        return default
    
    def _safe_int(self, value: str, default: int = 0) -> int:
        try:
            if value:
                return int(value)
        except (ValueError, AttributeError):
            pass
        return default
    
    def _human_size(self, size_gb: float) -> str:
        if size_gb >= 1024:
            return f"{size_gb/1024:.2f} TB"
        if size_gb >= 1:
            return f"{size_gb:.2f} GB"
        size_mb = size_gb * 1024
        if size_mb >= 1:
            return f"{size_mb:.2f} MB"
        return f"{size_gb*1024*1024:.2f} KB"
    
    def _format_status(self, status: LVMStatus) -> str:
        color_map = {
            LVMStatus.HEALTHY: Color.GREEN,
            LVMStatus.WARNING: Color.YELLOW,
            LVMStatus.CRITICAL: Color.RED,
            LVMStatus.UNKNOWN: Color.MAGENTA
        }
        return self._colorize(status.value, color_map.get(status, Color.WHITE))
    
    def check_lvm_installation(self) -> bool:
        output, code = self._run_command(["which", "lvm"])
        if code != 0 or not output:
            return False
        
        version_output, _ = self._run_command(["lvm", "version"])
        if version_output:
            lines = version_output.split('\n')
            if lines:
                print(f"{self._colorize('LVM Version:', Color.BOLD)} {lines[0]}")
        return True
    
    def check_disk_health(self) -> List[DiskInfo]:
        disks = []
        
        try:
            cmd = ["lsblk", "-d", "-o", "NAME,MODEL,SIZE", "-b", "--json"]
            output, code = self._run_command(cmd)
            if code == 0 and output:
                try:
                    data = json.loads(output)
                    for device in data.get('blockdevices', []):
                        disk = DiskInfo(
                            name=device.get('name', ''),
                            model=device.get('model', ''),
                            size_gb=float(device.get('size', 0)) / (1024**3) if device.get('size') else None
                        )
                        disks.append(disk)
                except json.JSONDecodeError:
                    pass
        except:
            pass
        
        for disk in disks:
            try:
                stat_path = f"/sys/block/{disk.name}/stat"
                if os.path.exists(stat_path):
                    with open(stat_path, 'r') as f:
                        stats = f.read().split()
                        if len(stats) >= 5:
                            disk.read_errors = self._safe_int(stats[3], 0)
                            disk.write_errors = self._safe_int(stats[7], 0)
            except:
                continue
        
        return disks
    
    def check_physical_volumes(self) -> List[PhysicalVolume]:
        cmd = ["pvs", "--units", "g", "--nosuffix", "--noheadings", 
               "--separator", "|", "-o", "pv_name,vg_name,pv_size,pv_free,pv_used,pv_attr,pv_uuid"]
        
        output, code = self._run_command(cmd)
        pvs = []
        
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if not line.strip():
                    continue
                
                fields = line.strip().split('|')
                if len(fields) >= 6:
                    try:
                        name = self._sanitize_lvm_name(fields[0].strip())
                        vg_name = self._sanitize_lvm_name(fields[1].strip()) if fields[1].strip() != "" else "<orphan>"
                        size_gb = self._safe_float(fields[2])
                        free_gb = self._safe_float(fields[3])
                        used_gb = self._safe_float(fields[4])
                        attributes = fields[5].strip()
                        uuid = fields[6].strip() if len(fields) > 6 else None
                        
                        used_percent = 0.0
                        if size_gb > 0:
                            used_percent = (used_gb / size_gb) * 100
                        
                        status = "ACTIVE" if "a" in attributes else "INACTIVE"
                        if "m" in attributes:
                            status = "MISSING"
                        elif "u" in attributes:
                            status = "UNKNOWN"
                        
                        disk_errors = 0
                        disk_model = None
                        base_disk = name.replace('/dev/', '')
                        if base_disk and os.path.exists(f"/sys/block/{base_disk.split('/')[0]}/stat"):
                            try:
                                with open(f"/sys/block/{base_disk.split('/')[0]}/stat", 'r') as f:
                                    stats = f.read().split()
                                    if len(stats) >= 5:
                                        disk_errors = self._safe_int(stats[3], 0) + self._safe_int(stats[7], 0)
                            except:
                                pass
                        
                        pv = PhysicalVolume(
                            name=name,
                            vg_name=vg_name,
                            size_gb=size_gb,
                            free_gb=free_gb,
                            used_percent=used_percent,
                            status=status,
                            attributes=attributes,
                            uuid=uuid,
                            disk_errors=disk_errors,
                            disk_model=disk_model
                        )
                        pvs.append(pv)
                    except Exception as e:
                        if self.verbose:
                            print(f"Error parsing PV line '{line}': {e}")
        
        return pvs
    
    def check_volume_groups(self) -> List[VolumeGroup]:
        cmd = ["vgs", "--units", "g", "--nosuffix", "--noheadings",
               "--separator", "|", "-o", "vg_name,vg_size,vg_free,vg_attr,pv_count,lv_count,vg_uuid,vg_extent_size,vg_lock_type,vg_lock_args"]
        
        output, code = self._run_command(cmd)
        vgs = []
        
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if not line.strip():
                    continue
                
                fields = line.strip().split('|')
                if len(fields) >= 6:
                    try:
                        name = self._sanitize_lvm_name(fields[0].strip())
                        size_gb = self._safe_float(fields[1])
                        free_gb = self._safe_float(fields[2])
                        attributes = fields[3].strip()
                        pv_count = self._safe_int(fields[4])
                        lv_count = self._safe_int(fields[5])
                        uuid = fields[6].strip() if len(fields) > 6 else None
                        extent_size = fields[7].strip() if len(fields) > 7 else None
                        lock_type = fields[8].strip() if len(fields) > 8 else None
                        lock_args = fields[9].strip() if len(fields) > 9 else None
                        
                        free_percent = 0.0
                        if size_gb > 0:
                            free_percent = (free_gb / size_gb) * 100
                        
                        vg = VolumeGroup(
                            name=name,
                            size_gb=size_gb,
                            free_gb=free_gb,
                            free_percent=free_percent,
                            pv_count=pv_count,
                            lv_count=lv_count,
                            attributes=attributes,
                            uuid=uuid,
                            extent_size=extent_size,
                            lock_type=lock_type,
                            lock_args=lock_args
                        )
                        vgs.append(vg)
                    except Exception as e:
                        if self.verbose:
                            print(f"Error parsing VG line '{line}': {e}")
        
        return vgs
    
    def check_logical_volumes(self) -> List[LogicalVolume]:
        cmd = ["lvs", "--units", "g", "--nosuffix", "--noheadings",
               "--separator", "|", "-o", "lv_name,vg_name,lv_size,lv_attr,pool_lv,origin,lv_uuid,segments,raid_sync_percent,cache_total_blocks,cache_used_blocks"]
        
        output, code = self._run_command(cmd)
        lvs = []
        
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if not line.strip():
                    continue
                
                fields = line.strip().split('|')
                if len(fields) >= 4:
                    try:
                        name = self._sanitize_lvm_name(fields[0].strip())
                        vg_name = self._sanitize_lvm_name(fields[1].strip())
                        size_gb = self._safe_float(fields[2])
                        attributes = fields[3].strip()
                        pool = fields[4].strip() if len(fields) > 4 and fields[4].strip() else None
                        origin = fields[5].strip() if len(fields) > 5 and fields[5].strip() else None
                        uuid = fields[6].strip() if len(fields) > 6 else None
                        segments = fields[7].strip() if len(fields) > 7 else None
                        raid_sync = self._safe_float(fields[8]) if len(fields) > 8 else None
                        cache_total = self._safe_int(fields[9]) if len(fields) > 9 else None
                        cache_used = self._safe_int(fields[10]) if len(fields) > 10 else None
                        
                        lv_type = "NORMAL"
                        if "t" in attributes:
                            lv_type = "THIN"
                        elif "s" in attributes:
                            lv_type = "SNAPSHOT"
                        elif "V" in attributes:
                            lv_type = "VIRTUAL"
                        elif "m" in attributes:
                            lv_type = "MIRRORED"
                        elif "r" in attributes:
                            lv_type = "RAID"
                        elif "c" in attributes or "C" in attributes:
                            lv_type = "CACHE"
                        
                        status = "ACTIVE" if "a" in attributes else "INACTIVE"
                        if "s" in attributes:
                            status = "SNAPSHOT"
                        
                        lv = LogicalVolume(
                            name=name,
                            vg_name=vg_name,
                            size_gb=size_gb,
                            lv_type=lv_type,
                            pool=pool,
                            origin=origin,
                            status=status,
                            attributes=attributes,
                            uuid=uuid,
                            segments=segments,
                            raid_sync_percent=raid_sync,
                            cache_total_blocks=cache_total,
                            cache_used_blocks=cache_used
                        )
                        lvs.append(lv)
                    except Exception as e:
                        if self.verbose:
                            print(f"Error parsing LV line '{line}': {e}")
        
        return lvs
    
    def check_thin_pools(self) -> List[ThinPool]:
        cmd = ["lvs", "--units", "g", "--nosuffix", "--noheadings",
               "--separator", "|", "-o", "lv_name,vg_name,data_percent,metadata_percent,thin_count,lv_uuid",
               "--select", "lv_attr=~[^t.*]"]
        
        output, code = self._run_command(cmd)
        pools = []
        
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if not line.strip():
                    continue
                
                fields = line.strip().split('|')
                if len(fields) >= 5:
                    try:
                        name = self._sanitize_lvm_name(fields[0].strip())
                        vg_name = self._sanitize_lvm_name(fields[1].strip())
                        data_percent = self._safe_float(fields[2])
                        metadata_percent = self._safe_float(fields[3])
                        thin_count = self._safe_int(fields[4])
                        uuid = fields[5].strip() if len(fields) > 5 else None
                        
                        pool = ThinPool(
                            name=name,
                            vg_name=vg_name,
                            data_percent=data_percent,
                            metadata_percent=metadata_percent,
                            thin_count=thin_count,
                            lv_uuid=uuid
                        )
                        pools.append(pool)
                    except Exception as e:
                        if self.verbose:
                            print(f"Error parsing thin pool line '{line}': {e}")
        
        return pools
    
    def check_cache_pools(self) -> List[CachePool]:
        cmd = ["lvs", "--units", "g", "--nosuffix", "--noheadings",
               "--separator", "|", "-o", "lv_name,vg_name,cache_total_blocks,cache_used_blocks,cache_dirty_blocks,lv_uuid",
               "--select", "lv_attr=~[^C.*]"]
        
        output, code = self._run_command(cmd)
        pools = []
        
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if not line.strip():
                    continue
                
                fields = line.strip().split('|')
                if len(fields) >= 4:
                    try:
                        name = self._sanitize_lvm_name(fields[0].strip())
                        vg_name = self._sanitize_lvm_name(fields[1].strip())
                        cache_total = self._safe_int(fields[2])
                        cache_used = self._safe_int(fields[3])
                        cache_dirty = self._safe_int(fields[4]) if len(fields) > 4 else None
                        uuid = fields[5].strip() if len(fields) > 5 else None
                        
                        pool = CachePool(
                            name=name,
                            vg_name=vg_name,
                            cache_total_blocks=cache_total,
                            cache_used_blocks=cache_used,
                            cache_dirty_blocks=cache_dirty,
                            lv_uuid=uuid
                        )
                        pools.append(pool)
                    except Exception as e:
                        if self.verbose:
                            print(f"Error parsing cache pool line '{line}': {e}")
        
        return pools
    
    def check_lvm_mounts(self) -> List[Dict[str, str]]:
        cmd = ["mount"]
        output, code = self._run_command(cmd)
        mounts = []
        
        if code == 0 and output:
            pattern = re.compile(r'^(/dev/(?:mapper|dm-\d+)[^ ]+) on ([^ ]+) type ([^ ]+)')
            
            for line in output.strip().split('\n'):
                match = pattern.search(line)
                if match:
                    device, mount_point, fs_type = match.group(1), match.group(2), match.group(3)
                    mounts.append({
                        'device': device,
                        'mount_point': mount_point,
                        'fs_type': fs_type
                    })
        
        return mounts
    
    def check_dm_devices(self) -> List[Dict[str, str]]:
        if not self._is_root:
            return []
        
        cmd = ["dmsetup", "status"]
        output, code = self._run_command(cmd)
        devices = []
        
        if code == 0 and output:
            for line in output.strip().split('\n'):
                if line.strip():
                    parts = line.split(':')
                    if len(parts) >= 2:
                        device_name = parts[0].strip()
                        status_info = ':'.join(parts[1:]).strip()
                        devices.append({
                            'name': device_name,
                            'status': status_info
                        })
        
        return devices
    
    def check_lvm_metadata_backup(self) -> Dict[str, Any]:
        backup_dirs = [
            ('/etc/lvm/backup', 'backup'),
            ('/etc/lvm/archive', 'archive'),
            ('/var/lib/lvm', 'var_lib')
        ]
        
        result = {
            'directories': [],
            'total_files': 0,
            'accessible': True
        }
        
        for dir_path, dir_name in backup_dirs:
            dir_info = {
                'path': dir_path,
                'name': dir_name,
                'exists': False,
                'accessible': False,
                'file_count': 0,
                'files': []
            }
            
            try:
                path = pathlib.Path(dir_path)
                if path.exists() and path.is_dir():
                    dir_info['exists'] = True
                    
                    try:
                        files = list(path.iterdir())
                        dir_info['file_count'] = len(files)
                        dir_info['accessible'] = True
                        
                        vg_files = [f.name for f in files if f.is_file() and not f.name.startswith('.')]
                        dir_info['files'] = sorted(vg_files)[:10]
                        
                        result['total_files'] += len(files)
                    except PermissionError:
                        dir_info['accessible'] = False
                        result['accessible'] = False
            except Exception:
                pass
            
            result['directories'].append(dir_info)
        
        return result
    
    def check_lvm_config(self) -> Dict[str, Any]:
        config_files = [
            ('/etc/lvm/lvm.conf', 'main_config'),
            ('/etc/lvm/lvmlocal.conf', 'local_config')
        ]
        
        result = {
            'files': [],
            'valid': True
        }
        
        for config_path, config_name in config_files:
            file_info = {
                'path': config_path,
                'name': config_name,
                'exists': False,
                'accessible': False,
                'lines': 0
            }
            
            try:
                path = pathlib.Path(config_path)
                if path.exists() and path.is_file():
                    file_info['exists'] = True
                    
                    try:
                        with open(config_path, 'r') as f:
                            lines = f.readlines()
                            file_info['lines'] = len(lines)
                            file_info['accessible'] = True
                            
                            for line in lines:
                                if line.strip().startswith('filter') or line.strip().startswith('global_filter'):
                                    result['filters'] = result.get('filters', [])
                                    result['filters'].append(line.strip())
                    except PermissionError:
                        file_info['accessible'] = False
                        result['valid'] = False
            except Exception:
                pass
            
            result['files'].append(file_info)
        
        return result
    
    def check_system_memory(self) -> Dict[str, Any]:
        result = {
            'total_gb': 0,
            'available_gb': 0,
            'free_gb': 0,
            'sufficient_for_thin': True
        }
        
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        meminfo[key.strip()] = value.strip()
            
            total_kb = self._safe_int(meminfo.get('MemTotal', '').replace('kB', '').strip(), 0)
            available_kb = self._safe_int(meminfo.get('MemAvailable', '').replace('kB', '').strip(), 0)
            free_kb = self._safe_int(meminfo.get('MemFree', '').replace('kB', '').strip(), 0)
            
            result['total_gb'] = total_kb / 1024 / 1024
            result['available_gb'] = available_kb / 1024 / 1024
            result['free_gb'] = free_kb / 1024 / 1024
            
            result['sufficient_for_thin'] = available_kb > 256 * 1024
            
        except Exception:
            pass
        
        return result
    
    def _display_table(self, title: str, headers: List[str], data: List[List[str]]) -> None:
        print(f"\n{self._colorize('=' * 80, Color.BOLD)}")
        print(f"{self._colorize(title.center(80), Color.BOLD)}")
        print(f"{self._colorize('=' * 80, Color.BOLD)}")
        
        if not data:
            print("No data available")
            return
        
        if HAS_TABULATE:
            print(tabulate(data, headers=headers, tablefmt="simple"))
        else:
            col_widths = [len(h) for h in headers]
            for row in data:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(cell)))
            
            header_row = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
            print(header_row)
            print("-" * len(header_row))
            
            for row in data:
                print(" | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)))
    
    def display_physical_volumes(self, pvs: List[PhysicalVolume]) -> None:
        if not pvs:
            print(f"\n{self._colorize('No physical volumes found', Color.YELLOW)}")
            return
        
        data = []
        for pv in pvs:
            status_display = self._format_status(pv.lvm_status)
            errors = str(pv.disk_errors) if pv.disk_errors else "0"
            data.append([
                pv.name,
                pv.vg_name,
                self._human_size(pv.size_gb),
                self._human_size(pv.free_gb),
                f"{pv.used_percent:.1f}%",
                pv.status,
                errors,
                status_display
            ])
        
        self._display_table(
            "PHYSICAL VOLUMES",
            ["PV Name", "VG Name", "Size", "Free", "Used %", "Status", "Disk Errors", "Health"],
            data
        )
    
    def display_volume_groups(self, vgs: List[VolumeGroup]) -> None:
        if not vgs:
            print(f"\n{self._colorize('No volume groups found', Color.YELLOW)}")
            return
        
        data = []
        for vg in vgs:
            status_display = self._format_status(vg.lvm_status)
            lock_info = vg.lock_type or "none"
            if lock_info == "none":
                lock_info = self._colorize("none", Color.GREEN)
            else:
                lock_info = self._colorize(lock_info, Color.YELLOW)
            
            data.append([
                vg.name,
                self._human_size(vg.size_gb),
                self._human_size(vg.free_gb),
                f"{vg.free_percent:.1f}%",
                str(vg.pv_count),
                str(vg.lv_count),
                lock_info,
                status_display
            ])
        
        self._display_table(
            "VOLUME GROUPS",
            ["VG Name", "Size", "Free", "Free %", "PVs", "LVs", "Lock", "Health"],
            data
        )
    
    def display_logical_volumes(self, lvs: List[LogicalVolume]) -> None:
        if not lvs:
            print(f"\n{self._colorize('No logical volumes found', Color.YELLOW)}")
            return
        
        data = []
        for lv in lvs:
            status_display = self._format_status(lv.lvm_status)
            sync_info = f"{lv.raid_sync_percent:.1f}%" if lv.raid_sync_percent is not None else "-"
            
            if lv.cache_total_blocks and lv.cache_used_blocks:
                cache_percent = (lv.cache_used_blocks / lv.cache_total_blocks * 100) if lv.cache_total_blocks > 0 else 0
                cache_info = f"{cache_percent:.1f}%"
            else:
                cache_info = "-"
            
            data.append([
                lv.vg_name,
                lv.name,
                self._human_size(lv.size_gb),
                lv.lv_type,
                lv.pool or "-",
                lv.origin or "-",
                sync_info,
                cache_info,
                status_display
            ])
        
        self._display_table(
            "LOGICAL VOLUMES",
            ["VG Name", "LV Name", "Size", "Type", "Pool", "Origin", "RAID Sync", "Cache %", "Health"],
            data
        )
    
    def display_thin_pools(self, pools: List[ThinPool]) -> None:
        if not pools:
            print(f"\n{self._colorize('No thin pools found', Color.YELLOW)}")
            return
        
        data = []
        for pool in pools:
            status_display = self._format_status(pool.lvm_status)
            data.append([
                pool.vg_name,
                pool.name,
                f"{pool.data_percent:.1f}%",
                f"{pool.metadata_percent:.1f}%",
                str(pool.thin_count),
                status_display
            ])
        
        self._display_table(
            "THIN POOLS",
            ["VG Name", "Pool Name", "Data Used %", "Meta Used %", "Thin Volumes", "Health"],
            data
        )
    
    def display_cache_pools(self, pools: List[CachePool]) -> None:
        if not pools:
            return
        
        data = []
        for pool in pools:
            status_display = self._format_status(pool.lvm_status)
            usage = (pool.cache_used_blocks / pool.cache_total_blocks * 100) if pool.cache_total_blocks > 0 else 0
            dirty = f"{pool.cache_dirty_blocks}" if pool.cache_dirty_blocks is not None else "-"
            
            data.append([
                pool.vg_name,
                pool.name,
                str(pool.cache_total_blocks),
                str(pool.cache_used_blocks),
                f"{usage:.1f}%",
                dirty,
                status_display
            ])
        
        self._display_table(
            "CACHE POOLS",
            ["VG Name", "Pool Name", "Total Blocks", "Used Blocks", "Usage %", "Dirty Blocks", "Health"],
            data
        )
    
    def display_mounts(self, mounts: List[Dict[str, str]]) -> None:
        if not mounts:
            print(f"\n{self._colorize('No LVM mounts found', Color.YELLOW)}")
            return
        
        data = []
        for mount in mounts:
            data.append([
                mount['device'],
                mount['mount_point'],
                mount['fs_type']
            ])
        
        self._display_table(
            "MOUNTED LVM VOLUMES",
            ["Device", "Mount Point", "Filesystem Type"],
            data
        )
    
    def display_dm_devices(self, devices: List[Dict[str, str]]) -> None:
        if not devices:
            print(f"\n{self._colorize('No device mapper devices found', Color.YELLOW)}")
            return
        
        data = []
        for device in devices:
            status = device['status']
            if len(status) > 50:
                status = status[:47] + "..."
            data.append([
                device['name'],
                status
            ])
        
        self._display_table(
            "DEVICE MAPPER DEVICES",
            ["Device Name", "Status"],
            data
        )
    
    def display_metadata_backup(self, backup_info: Dict[str, Any]) -> None:
        data = []
        for dir_info in backup_info['directories']:
            status = self._colorize("OK", Color.GREEN) if dir_info['accessible'] else self._colorize("NO ACCESS", Color.RED)
            exists = self._colorize("YES", Color.GREEN) if dir_info['exists'] else self._colorize("NO", Color.YELLOW)
            data.append([
                dir_info['path'],
                exists,
                str(dir_info['file_count']),
                status
            ])
        
        self._display_table(
            "LVM METADATA BACKUP STATUS",
            ["Directory", "Exists", "File Count", "Access"],
            data
        )
        
        if backup_info['total_files'] > 0:
            print(f"\nTotal backup files: {backup_info['total_files']}")
    
    def display_disk_health(self, disks: List[DiskInfo]) -> None:
        if not disks:
            return
        
        data = []
        for disk in disks:
            size_str = self._human_size(disk.size_gb) if disk.size_gb else "N/A"
            error_str = f"{disk.read_errors}/{disk.write_errors}"
            health = LVMStatus.HEALTHY
            if disk.read_errors > 10 or disk.write_errors > 10:
                health = LVMStatus.CRITICAL
            elif disk.read_errors > 0 or disk.write_errors > 0:
                health = LVMStatus.WARNING
            
            status_display = self._format_status(health)
            data.append([
                disk.name,
                disk.model or "N/A",
                size_str,
                error_str,
                status_display
            ])
        
        self._display_table(
            "DISK HEALTH",
            ["Disk", "Model", "Size", "R/W Errors", "Health"],
            data
        )
    
    def generate_health_report(self, pvs, vgs, lvs, thin_pools, cache_pools, disks) -> Tuple[List[str], List[str]]:
        issues = []
        warnings = []
        
        for pv in pvs:
            if pv.lvm_status == LVMStatus.CRITICAL:
                issues.append(f"Critical PV: {pv.name} ({pv.status})")
            elif pv.lvm_status == LVMStatus.WARNING:
                warnings.append(f"Warning PV: {pv.name} ({pv.status})")
            if pv.disk_errors and pv.disk_errors > 10:
                issues.append(f"Critical Disk Errors: {pv.name} ({pv.disk_errors} errors)")
            elif pv.disk_errors and pv.disk_errors > 0:
                warnings.append(f"Disk Errors: {pv.name} ({pv.disk_errors} errors)")
        
        for vg in vgs:
            if vg.lvm_status == LVMStatus.CRITICAL:
                if "p" in vg.attributes or "x" in vg.attributes:
                    issues.append(f"Critical VG: {vg.name} (partial/missing)")
                elif vg.free_percent < 5:
                    issues.append(f"Critical VG: {vg.name} (only {vg.free_percent:.1f}% free)")
            elif vg.lvm_status == LVMStatus.WARNING:
                if vg.free_percent < 10:
                    warnings.append(f"Warning VG: {vg.name} (low free space: {vg.free_percent:.1f}%)")
                if vg.lock_type and vg.lock_type not in ["normal", "None", ""]:
                    warnings.append(f"Warning VG: {vg.name} (locked: {vg.lock_type})")
        
        for lv in lvs:
            if lv.lvm_status == LVMStatus.CRITICAL:
                issues.append(f"Critical LV: {lv.vg_name}/{lv.name} (inactive)")
            elif lv.lvm_status == LVMStatus.WARNING:
                if "m" in lv.attributes:
                    warnings.append(f"Warning LV: {lv.vg_name}/{lv.name} (mirrored issues)")
                if "r" in lv.attributes and lv.raid_sync_percent is not None and lv.raid_sync_percent < 100:
                    warnings.append(f"Warning LV: {lv.vg_name}/{lv.name} (RAID sync: {lv.raid_sync_percent:.1f}%)")
        
        for pool in thin_pools:
            if pool.lvm_status == LVMStatus.CRITICAL:
                issues.append(f"Critical Thin Pool: {pool.vg_name}/{pool.name} (over {pool.data_percent:.1f}% used)")
            elif pool.lvm_status == LVMStatus.WARNING:
                warnings.append(f"Warning Thin Pool: {pool.vg_name}/{pool.name} ({pool.data_percent:.1f}% used)")
        
        for pool in cache_pools:
            if pool.lvm_status == LVMStatus.CRITICAL:
                issues.append(f"Critical Cache Pool: {pool.vg_name}/{pool.name} (over 95% full)")
            elif pool.lvm_status == LVMStatus.WARNING:
                warnings.append(f"Warning Cache Pool: {pool.vg_name}/{pool.name} (over 85% full)")
        
        for disk in disks:
            if disk.read_errors > 10 or disk.write_errors > 10:
                issues.append(f"Critical Disk: {disk.name} ({disk.read_errors}/{disk.write_errors} R/W errors)")
            elif disk.read_errors > 0 or disk.write_errors > 0:
                warnings.append(f"Warning Disk: {disk.name} ({disk.read_errors}/{disk.write_errors} R/W errors)")
        
        return issues, warnings
    
    def display_summary(self, pvs, vgs, lvs, thin_pools, cache_pools, disks, mounts, dm_devices, issues, warnings) -> None:
        total_size_gb = sum(vg.size_gb for vg in vgs)
        total_free_gb = sum(vg.free_gb for vg in vgs)
        total_free_percent = (total_free_gb / total_size_gb * 100) if total_size_gb > 0 else 0
        
        healthy_pvs = sum(1 for pv in pvs if pv.lvm_status == LVMStatus.HEALTHY)
        healthy_vgs = sum(1 for vg in vgs if vg.lvm_status == LVMStatus.HEALTHY)
        healthy_lvs = sum(1 for lv in lvs if lv.lvm_status == LVMStatus.HEALTHY)
        
        memory_info = self.check_system_memory()
        
        data = [
            ["Physical Volumes", f"{healthy_pvs}/{len(pvs)} healthy"],
            ["Volume Groups", f"{healthy_vgs}/{len(vgs)} healthy"],
            ["Logical Volumes", f"{healthy_lvs}/{len(lvs)} healthy"],
            ["Thin Pools", str(len(thin_pools))],
            ["Cache Pools", str(len(cache_pools))],
            ["Mounted Volumes", str(len(mounts))],
            ["DM Devices", str(len(dm_devices))],
            ["Total LVM Storage", self._human_size(total_size_gb)],
            ["Total Free Space", f"{self._human_size(total_free_gb)} ({total_free_percent:.1f}%)"],
            ["System Memory", f"{memory_info['available_gb']:.1f} GB available"],
            ["Sufficient for thin", "YES" if memory_info['sufficient_for_thin'] else self._colorize("NO", Color.YELLOW)]
        ]
        
        overall_status = LVMStatus.HEALTHY
        if issues:
            overall_status = LVMStatus.CRITICAL
        elif warnings:
            overall_status = LVMStatus.WARNING
        
        print(f"\n{self._colorize('LVM SYSTEM SUMMARY', Color.BOLD)}")
        print(f"Overall Status: {self._format_status(overall_status)}")
        print(f"Check Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        
        if not self._is_root:
            print(f"{self._colorize('Note: Not running as root - some information may be limited', Color.YELLOW)}")
        
        self._display_table("", ["Component", "Status"], data)
        
        if warnings:
            print(f"\n{self._colorize('Warnings:', Color.YELLOW)}")
            for warning in warnings:
                print(f"  • {warning}")
        
        if issues:
            print(f"\n{self._colorize('Critical Issues:', Color.RED)}")
            for issue in issues:
                print(f"  • {issue}")
        
        if issues:
            print(f"\n{self._colorize('Recommended Actions:', Color.CYAN)}")
            for issue in issues:
                if "Critical VG" in issue and "only" in issue:
                    print(f"  • Consider extending volume group or cleaning up unused LVs")
                elif "Critical PV" in issue:
                    print(f"  • Check physical disk health and connectivity")
                elif "Critical Thin Pool" in issue:
                    print(f"  • Extend thin pool or migrate data to free up space")
                elif "Critical Disk Errors" in issue:
                    print(f"  • Replace failing disk immediately")
    
    def run_full_check(self) -> LVMHealthCheck:
        print(f"{self._colorize('LVM SYSTEM HEALTH CHECK', Color.BOLD + Color.CYAN)}")
        print(f"{self._colorize('=' * 80, Color.BOLD)}\n")
        
        start_time = time.time()
        
        if not self._is_root:
            print(f"{self._colorize('Warning: Not running as root. Some information may be limited.', Color.YELLOW)}")
            print(f"{self._colorize('         Run with sudo for complete details.', Color.YELLOW)}\n")
        
        if not self.check_lvm_installation():
            print(f"{self._colorize('Error: LVM is not installed or not accessible', Color.RED)}")
            sys.exit(1)
        
        print(f"{self._colorize('Collecting LVM information...', Color.BLUE)}")
        
        disks = self.check_disk_health()
        pvs = self.check_physical_volumes()
        vgs = self.check_volume_groups()
        lvs = self.check_logical_volumes()
        thin_pools = self.check_thin_pools()
        cache_pools = self.check_cache_pools()
        mounts = self.check_lvm_mounts()
        dm_devices = self.check_dm_devices()
        metadata_backup = self.check_lvm_metadata_backup()
        
        issues, warnings = self.generate_health_report(pvs, vgs, lvs, thin_pools, cache_pools, disks)
        
        self.display_physical_volumes(pvs)
        self.display_volume_groups(vgs)
        self.display_logical_volumes(lvs)
        self.display_thin_pools(thin_pools)
        if cache_pools:
            self.display_cache_pools(cache_pools)
        self.display_disk_health(disks)
        self.display_mounts(mounts)
        if dm_devices:
            self.display_dm_devices(dm_devices)
        self.display_metadata_backup(metadata_backup)
        self.display_summary(pvs, vgs, lvs, thin_pools, cache_pools, disks, mounts, dm_devices, issues, warnings)
        
        elapsed = time.time() - start_time
        print(f"\n{self._colorize(f'Check completed in {elapsed:.2f} seconds', Color.BLUE)}")
        
        self.health_check = LVMHealthCheck(
            pvs=pvs,
            vgs=vgs,
            lvs=lvs,
            thin_pools=thin_pools,
            cache_pools=cache_pools,
            disks=disks,
            mounts=mounts,
            dm_devices=dm_devices,
            metadata_backup=metadata_backup,
            timestamp=time.time(),
            issues=issues,
            warnings=warnings
        )
        
        return self.health_check
    
    def export_json(self, filename: str = "lvm_state.json") -> bool:
        if not self.health_check:
            print(f"{self._colorize('Error: No health check data available', Color.RED)}")
            return False
        
        class LVMEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, LVMStatus):
                    return obj.value
                if isinstance(obj, (PhysicalVolume, VolumeGroup, LogicalVolume, ThinPool, CachePool, DiskInfo)):
                    return asdict(obj)
                if hasattr(obj, '__dict__'):
                    return obj.__dict__
                return super().default(obj)
        
        try:
            data = {
                'timestamp': self.health_check.timestamp,
                'overall_status': self.health_check.overall_status.value,
                'issues': self.health_check.issues,
                'warnings': self.health_check.warnings,
                'physical_volumes': [asdict(pv) for pv in self.health_check.pvs],
                'volume_groups': [asdict(vg) for vg in self.health_check.vgs],
                'logical_volumes': [asdict(lv) for lv in self.health_check.lvs],
                'thin_pools': [asdict(pool) for pool in self.health_check.thin_pools],
                'cache_pools': [asdict(pool) for pool in self.health_check.cache_pools],
                'disks': [asdict(disk) for disk in self.health_check.disks],
                'mounts': self.health_check.mounts,
                'dm_devices': self.health_check.dm_devices,
                'metadata_backup': self.health_check.metadata_backup
            }
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2, cls=LVMEncoder)
            
            print(f"\n{self._colorize('✓', Color.GREEN)} Data exported to {filename}")
            return True
            
        except Exception as e:
            print(f"{self._colorize('Error exporting JSON:', Color.RED)} {e}")
            return False
    
    def export_prometheus(self, filename: str = "lvm_metrics.prom") -> bool:
        if not self.health_check:
            return False
        
        try:
            timestamp = int(time.time() * 1000)
            metrics = []
            
            metrics.append(f"# HELP lvm_health_check LVM health check metrics")
            metrics.append(f"# TYPE lvm_health_check gauge")
            metrics.append(f'lvm_health_check{{type="overall"}} {1 if self.health_check.overall_status == LVMStatus.HEALTHY else 0} {timestamp}')
            
            metrics.append(f"\n# HELP lvm_volume_group_free_percent Volume group free space percentage")
            metrics.append(f"# TYPE lvm_volume_group_free_percent gauge")
            for vg in self.health_check.vgs:
                metrics.append(f'lvm_volume_group_free_percent{{vg="{vg.name}"}} {vg.free_percent} {timestamp}')
            
            metrics.append(f"\n# HELP lvm_volume_group_size Volume group total size in GB")
            metrics.append(f"# TYPE lvm_volume_group_size gauge")
            for vg in self.health_check.vgs:
                metrics.append(f'lvm_volume_group_size{{vg="{vg.name}"}} {vg.size_gb} {timestamp}')
            
            metrics.append(f"\n# HELP lvm_thin_pool_usage Thin pool usage percentage")
            metrics.append(f"# TYPE lvm_thin_pool_usage gauge")
            for pool in self.health_check.thin_pools:
                metrics.append(f'lvm_thin_pool_usage{{pool="{pool.vg_name}/{pool.name}",type="data"}} {pool.data_percent} {timestamp}')
                metrics.append(f'lvm_thin_pool_usage{{pool="{pool.vg_name}/{pool.name}",type="metadata"}} {pool.metadata_percent} {timestamp}')
            
            metrics.append(f"\n# HELP lvm_disk_errors Disk I/O errors")
            metrics.append(f"# TYPE lvm_disk_errors counter")
            for disk in self.health_check.disks:
                metrics.append(f'lvm_disk_errors{{disk="{disk.name}",type="read"}} {disk.read_errors} {timestamp}')
                metrics.append(f'lvm_disk_errors{{disk="{disk.name}",type="write"}} {disk.write_errors} {timestamp}')
            
            metrics.append(f"\n# HELP lvm_cache_pool_usage Cache pool usage percentage")
            metrics.append(f"# TYPE lvm_cache_pool_usage gauge")
            for pool in self.health_check.cache_pools:
                usage = (pool.cache_used_blocks / pool.cache_total_blocks * 100) if pool.cache_total_blocks > 0 else 0
                metrics.append(f'lvm_cache_pool_usage{{pool="{pool.vg_name}/{pool.name}"}} {usage} {timestamp}')
            
            with open(filename, 'w') as f:
                f.write("\n".join(metrics))
            
            print(f"\n{self._colorize('✓', Color.GREEN)} Prometheus metrics exported to {filename}")
            return True
            
        except Exception as e:
            print(f"{self._colorize('Error exporting Prometheus metrics:', Color.RED)} {e}")
            return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Check LVM state on Linux system',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--json', '-j', action='store_true',
                       help='Export results to JSON')
    parser.add_argument('--prometheus', '-p', action='store_true',
                       help='Export Prometheus metrics')
    parser.add_argument('--output', '-o', default='lvm_state.json',
                       help='Output JSON filename')
    parser.add_argument('--prom-file', default='lvm_metrics.prom',
                       help='Output Prometheus filename')
    parser.add_argument('--no-color', action='store_true',
                       help='Disable colored output')
    parser.add_argument('--timeout', type=int, default=30,
                       help='Command timeout in seconds')
    parser.add_argument('--cache-ttl', type=int, default=300,
                       help='Cache TTL in seconds')
    
    args = parser.parse_args()
    
    try:
        checker = LVMStateChecker(
            verbose=args.verbose,
            color=not args.no_color,
            timeout=args.timeout
        )
        checker._cache_ttl = args.cache_ttl
        
        health_check = checker.run_full_check()
        
        if args.json:
            checker.export_json(args.output)
        
        if args.prometheus:
            checker.export_prometheus(args.prom_file)
        
        exit_code = 0
        if health_check.overall_status == LVMStatus.CRITICAL:
            exit_code = 2
        elif health_check.overall_status == LVMStatus.WARNING:
            exit_code = 1
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print(f"\n{Color.YELLOW}Interrupted by user{Color.RESET}")
        sys.exit(130)
    except Exception as e:
        print(f"{Color.RED}Error: {e}{Color.RESET}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
