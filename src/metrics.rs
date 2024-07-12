use std::fmt::Display;
use std::io;

use once_cell::sync::Lazy;
use prometheus::{
    self, proto, labels, opts, register, register_int_counter, register_int_gauge, Counter, Gauge,
    IntCounter, IntGauge, PullingGauge,
    core::{Collector, Desc},
    Opts,
};

pub fn register_metric(
    name: &str,
    help: &str,
    f: &'static (dyn Fn() -> f64 + Sync + Send),
) -> io::Result<()> {
    let metric = PullingGauge::new(name, help, Box::new(f)).unwrap();
    register(Box::new(metric)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

pub fn register_constant<T>(name: &str, help: &str, value: T) -> io::Result<()>
where
    T: TryInto<i64> + Display + Copy,
{
    let g = register_int_gauge!(name, help).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    g.set(value.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{value} does not fit into i64"),
        )
    })?);
    Ok(())
}

pub fn register_label(name: &str, help: &str, label: &str, value: &str) -> io::Result<()> {
    let opts = opts!(name, help, labels! {label => value});
    let g = IntGauge::with_opts(opts).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    g.set(1);
    register(Box::new(g)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

pub static METRIC_REQUEST_COUNT: Lazy<IntCounter> =
    Lazy::new(|| register_int_counter!("piproxy_request_count", "Number of requests").unwrap());

pub static METRIC_REQUEST_ERROR_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_request_error_count",
        "Number of requests with errors"
    )
    .unwrap()
});

pub static METRIC_UPSTREAM_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_upstream_bytes",
        "Number of bytes downloaded from upstream"
    )
    .unwrap()
});

pub static METRIC_DOWNSTREAM_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_downstream_bytes",
        "Number of bytes sent to downstream"
    )
    .unwrap()
});

pub static METRIC_CACHE_HITS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_hit_count",
        "Number of full and partial cache hits"
    )
    .unwrap()
});

pub static METRIC_CACHE_HITS_FULL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("piproxy_cache_hit_full_count", "Number of full cache hits").unwrap()
});

pub static METRIC_CACHE_HITS_PARTIAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_hit_partial_count",
        "Number of partial cache hits"
    )
    .unwrap()
});

pub static METRIC_CACHE_MISSES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("piproxy_cache_miss_count", "Number of cache misses").unwrap()
});

pub static METRIC_CACHE_PURGES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("piproxy_cache_purge_count", "Number of cache purges").unwrap()
});

pub static METRIC_CACHE_LOOKUP_ERRORS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_lookup_error_count",
        "Number of cache lookup errors"
    )
    .unwrap()
});

pub static METRIC_CACHE_META_UPDATES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_meta_update_count",
        "Number of cache meta updates"
    )
    .unwrap()
});

pub static METRIC_WARN_STALE_PARTIAL_EXISTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_warn_stale_partial_count",
        "Number of warnings for existing stale partial files"
    )
    .unwrap()
});

pub static METRIC_WARN_MISSING_DATA_FILE: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_warn_missing_data_file_count",
        "Number of warnings for missing data files"
    )
    .unwrap()
});

pub fn init() -> io::Result<()> {
    METRIC_REQUEST_COUNT.get();
    METRIC_REQUEST_ERROR_COUNT.get();
    METRIC_UPSTREAM_BYTES.get();
    METRIC_DOWNSTREAM_BYTES.get();
    METRIC_CACHE_HITS.get();
    METRIC_CACHE_HITS_FULL.get();
    METRIC_CACHE_HITS_PARTIAL.get();
    METRIC_CACHE_MISSES.get();
    METRIC_CACHE_PURGES.get();
    METRIC_CACHE_LOOKUP_ERRORS.get();
    METRIC_CACHE_META_UPDATES.get();
    METRIC_WARN_STALE_PARTIAL_EXISTS.get();
    METRIC_WARN_MISSING_DATA_FILE.get();
    register_default_process_collector()
}

fn register_default_process_collector() -> io::Result<()> {
    let pc = ProcessCollector::new("");
    prometheus::register(Box::new(pc)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

/* Adapted from
https://github.com/tikv/rust-prometheus/blob/master/src/process_collector.rs

LICENSE:
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "{}"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright 2019 TiKV Project Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
/// A modified ProcessCollector from prometheus crate.
/// Modificaitons:
/// - Always collects process metrics for self, rather than some PID
/// - process_cpu_seconds_total and process_start_time_seconds are f64
#[derive(Debug)]
pub struct ProcessCollector {
    descs: Vec<Desc>,
    cpu_total: Counter,
    open_fds: IntGauge,
    max_fds: IntGauge,
    vsize: IntGauge,
    rss: IntGauge,
    start_time: Gauge,
    threads: IntGauge,
}

const METRICS_NUMBER: usize = 7;
static CLK_TCK: Lazy<f64> = Lazy::new(|| unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64);
static PAGESIZE: Lazy<i64> = Lazy::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.into());

impl ProcessCollector {
    pub fn new<S: Into<String>>(namespace: S) -> ProcessCollector {
        let namespace = namespace.into();
        let mut descs = Vec::new();

        let cpu_total = Counter::with_opts(
            Opts::new(
                "process_cpu_seconds_total",
                "Total user and system CPU time spent in \
                 seconds.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(cpu_total.desc().into_iter().cloned());

        let open_fds = IntGauge::with_opts(
            Opts::new("process_open_fds", "Number of open file descriptors.")
                .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(open_fds.desc().into_iter().cloned());

        let max_fds = IntGauge::with_opts(
            Opts::new(
                "process_max_fds",
                "Maximum number of open file descriptors.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(max_fds.desc().into_iter().cloned());

        let vsize = IntGauge::with_opts(
            Opts::new(
                "process_virtual_memory_bytes",
                "Virtual memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = IntGauge::with_opts(
            Opts::new(
                "process_resident_memory_bytes",
                "Resident memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        let start_time = Gauge::with_opts(
            Opts::new(
                "process_start_time_seconds",
                "Start time of the process since unix epoch \
                 in seconds.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();

        // proc_start_time init once because it is immutable
        if let Ok(boot_time) = procfs::boot_time_secs() {
            if let Ok(stat) = procfs::process::Process::myself().and_then(|p| p.stat()) {
                start_time.set(stat.starttime as f64 / *CLK_TCK + boot_time as f64);
            }
        }
        descs.extend(start_time.desc().into_iter().cloned());

        let threads = IntGauge::with_opts(
            Opts::new("process_threads", "Number of OS threads in the process.")
                .namespace(namespace),
        )
        .unwrap();
        descs.extend(threads.desc().into_iter().cloned());

        ProcessCollector {
            descs,
            cpu_total,
            open_fds,
            max_fds,
            vsize,
            rss,
            start_time,
            threads,
        }
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let p = match procfs::process::Process::myself() {
            Ok(p) => p,
            Err(..) => {
                return Vec::new();
            }
        };

        // file descriptors
        if let Ok(fd_count) = p.fd_count() {
            self.open_fds.set(fd_count as i64);
        }
        if let Ok(limits) = p.limits() {
            if let procfs::process::LimitValue::Value(max) = limits.max_open_files.soft_limit {
                self.max_fds.set(max as i64)
            }
        }

        let mut cpu_total_mfs = None;
        if let Ok(stat) = p.stat() {
            // memory
            self.vsize.set(stat.vsize as i64);
            self.rss.set((stat.rss as i64) * *PAGESIZE);

            // cpu
            let total = (stat.utime + stat.stime) as f64 / *CLK_TCK;
            let past = self.cpu_total.get();
            let inc = total - past;
            // only update if positive change
            if inc > 0f64 {
                self.cpu_total.inc_by(inc);
            }
            cpu_total_mfs = Some(self.cpu_total.collect());

            // threads
            self.threads.set(stat.num_threads);
        }

        // collect MetricFamilys.
        let mut mfs = Vec::with_capacity(METRICS_NUMBER);
        if let Some(cpu) = cpu_total_mfs {
            mfs.extend(cpu);
        }
        mfs.extend(self.open_fds.collect());
        mfs.extend(self.max_fds.collect());
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.start_time.collect());
        mfs.extend(self.threads.collect());
        mfs
    }
}
