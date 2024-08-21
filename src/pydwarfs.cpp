#include <memory>

#include <fmt/format.h>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl/filesystem.h>

#include <dwarfs/history.h>
#include <dwarfs/logger.h>
#include <dwarfs/os_access_generic.h>
#include <dwarfs/performance_monitor.h>
#include <dwarfs/reader/filesystem_options.h>
#include <dwarfs/reader/filesystem_v2.h>
#include <dwarfs/reader/fsinfo_options.h>
#include <dwarfs/version.h>
#include <dwarfs/vfs_stat.h>

#include "pybind11_json.hpp"

namespace dr = dwarfs::reader;
namespace fs = std::filesystem;
namespace py = pybind11;

namespace {

class py_logger : public dwarfs::logger {
 public:
  py_logger(level_type threshold = level_type::INFO)
      : threshold_{threshold} {
    if (threshold >= level_type::DEBUG) {
      set_policy<dwarfs::debug_logger_policy>();
    } else {
      set_policy<dwarfs::prod_logger_policy>();
    }
  }

  void write(level_type level, std::string const& msg, char const* file,
             int line) override {
    if (level <= threshold_ || level == FATAL) {
      PYBIND11_OVERRIDE_PURE(
          void,           // Return type
          dwarfs::logger, // Parent class
          write,          // Name of function in C++ (must match Python name)
          level, msg, file, line // Argument(s)
      );
    }
  }

 private:
  level_type threshold_;
};

class py_filesystem {
 public:
  py_filesystem(dwarfs::logger& logger, dwarfs::os_access const& os_access,
                fs::path const& path,
                std::shared_ptr<dwarfs::performance_monitor> perfmon)
      : logger_{py::cast(logger)}
      , os_access_{py::cast(os_access)}
      , fs_{std::make_unique<dr::filesystem_v2>(logger, os_access, path,
                                                dr::filesystem_options{},
                                                std::move(perfmon))} {}

  py_filesystem& enter() { return *this; }

  void exit(py::object exc_type, py::object exc_value, py::object traceback) {
    fs_.reset();
    if (exc_type.is_none()) {
      return;
    }
    py::print("Exception occurred, type: ", exc_type);
    py::print("Exception occurred, value: ", exc_value);
    py::print("Exception occurred, traceback: ", traceback);
  }

  std::string dump(dr::fsinfo_options const& options) const {
    return fs_->dump(options);
  }

  dr::dir_entry_view root() const { return fs_->root(); }

  std::optional<dr::dir_entry_view> find(std::string_view name) const {
    return fs_->find(name);
  }

  int open(dr::inode_view iv) const { return fs_->open(iv); }

  py::bytes read(uint32_t inode, size_t size, dwarfs::file_off_t offset) const {
    std::string result;
    {
      py::gil_scoped_release release;
      result = fs_->read_string(inode, size, offset);
    }
    return py::bytes(result);
  }

  std::vector<std::future<dr::block_range>>
  readv(uint32_t inode, size_t size, dwarfs::file_off_t offset) const {
    py::gil_scoped_release release; // TODO: ???
    return fs_->readv(inode, size, offset);
  }

  std::optional<dr::directory_view> opendir(dr::inode_view entry) const {
    return fs_->opendir(entry);
  }

  std::optional<dr::dir_entry_view>
  readdir(dr::directory_view dir, size_t offset) const {
    return fs_->readdir(dir, offset);
  }

  size_t dirsize(dr::directory_view dir) const { return fs_->dirsize(dir); }

  void walk(std::function<void(dr::dir_entry_view)> const& func) const {
    fs_->walk(func);
  }

  void
  walk_data_order(std::function<void(dr::dir_entry_view)> const& func) const {
    fs_->walk_data_order(func);
  }

  nlohmann::json info(dr::fsinfo_options const& options) const {
    return fs_->info_as_json(options);
  }

  dwarfs::file_stat getattr(dr::inode_view iv) const {
    return fs_->getattr(iv);
  }

  nlohmann::json get_inode_info(dr::inode_view iv) const {
    return fs_->get_inode_info(iv);
  }

  nlohmann::json get_metadata() const {
    return nlohmann::json::parse(fs_->serialize_metadata_as_json(true));
  }

  std::string readlink(dr::inode_view iv, dr::readlink_mode mode) const {
    return fs_->readlink(iv, mode);
  }

  dwarfs::vfs_stat statvfs() const {
    dwarfs::vfs_stat result;
    fs_->statvfs(&result);
    return result;
  }

  nlohmann::json get_history() const { return fs_->get_history().as_json(); }

 private:
  py::object logger_;
  py::object os_access_;
  std::unique_ptr<dr::filesystem_v2> fs_;
};

std::shared_ptr<dwarfs::performance_monitor>
create_performance_monitor(std::unordered_set<std::string> const& names) {
  return dwarfs::performance_monitor::create(names);
}

} // namespace

PYBIND11_MODULE(_pydwarfs, m) {
  py::class_<dwarfs::logger, py_logger> logger(m, "logger");
  logger.def(py::init<dwarfs::logger::level_type>())
      .def("write", &dwarfs::logger::write);

  py::enum_<dwarfs::logger::level_type>(logger, "level_type")
      .value("FATAL", dwarfs::logger::level_type::FATAL)
      .value("ERROR", dwarfs::logger::level_type::ERROR)
      .value("WARN", dwarfs::logger::level_type::WARN)
      .value("INFO", dwarfs::logger::level_type::INFO)
      .value("VERBOSE", dwarfs::logger::level_type::VERBOSE)
      .value("DEBUG", dwarfs::logger::level_type::DEBUG)
      .value("TRACE", dwarfs::logger::level_type::TRACE)
      .export_values();

  py::class_<dwarfs::stream_logger, dwarfs::logger>(m, "stream_logger")
      .def(py::init<>());

  py::class_<dwarfs::os_access>(m, "os_access");

  py::class_<dwarfs::os_access_generic, dwarfs::os_access>(m,
                                                           "os_access_generic")
      .def(py::init<>());

  auto mr = m.def_submodule("reader");

  py::enum_<dr::fsinfo_feature>(mr, "fsinfo_feature")
      .value("version", dr::fsinfo_feature::version)
      .value("history", dr::fsinfo_feature::history)
      .value("metadata_summary", dr::fsinfo_feature::metadata_summary)
      .value("metadata_details", dr::fsinfo_feature::metadata_details)
      .value("metadata_full_dump", dr::fsinfo_feature::metadata_full_dump)
      .value("frozen_analysis", dr::fsinfo_feature::frozen_analysis)
      .value("frozen_layout", dr::fsinfo_feature::frozen_layout)
      .value("directory_tree", dr::fsinfo_feature::directory_tree)
      .value("section_details", dr::fsinfo_feature::section_details)
      .value("chunk_details", dr::fsinfo_feature::chunk_details)
      .export_values();

  py::class_<dr::fsinfo_features>(mr, "fsinfo_features")
      .def(py::init<>())
      .def(py::init<std::initializer_list<dr::fsinfo_feature>>())
      .def("for_level", &dr::fsinfo_features::for_level)
      .def("__repr__", &dr::fsinfo_features::to_string);

  py::enum_<dr::block_access_level>(mr, "block_access_level")
      .value("no_access", dr::block_access_level::no_access)
      .value("no_verify", dr::block_access_level::no_verify)
      .value("unrestricted", dr::block_access_level::unrestricted)
      .export_values();

  py::class_<dr::fsinfo_options>(mr, "fsinfo_options")
      .def(py::init<>())
      .def_readwrite("features", &dr::fsinfo_options::features)
      .def_readwrite("block_access", &dr::fsinfo_options::block_access);

  py::class_<dr::inode_view>(mr, "inode_view")
      .def(py::init<>())
      // .def("mode", &dr::inode_view::mode)
      .def("mode_string", &dr::inode_view::mode_string)
      .def("perm_string", &dr::inode_view::perm_string)
      // .def("type", &dr::inode_view::type)
      .def("is_regular_file", &dr::inode_view::is_regular_file)
      .def("is_directory", &dr::inode_view::is_directory)
      .def("is_symlink", &dr::inode_view::is_symlink)
      .def("getuid", &dr::inode_view::getuid)
      .def("getgid", &dr::inode_view::getgid)
      .def("inode_num", &dr::inode_view::inode_num)
      .def("__repr__", [](dr::inode_view const& iv) {
        return fmt::format("inode_view(inode={})", iv.inode_num());
      });

  py::class_<dr::dir_entry_view>(mr, "dir_entry_view")
      .def(py::init<>())
      .def("name", &dr::dir_entry_view::name)
      .def("inode", &dr::dir_entry_view::inode)
      .def("is_root", &dr::dir_entry_view::is_root)
      .def("parent", &dr::dir_entry_view::parent)
      .def("path", &dr::dir_entry_view::path)
      .def("unix_path", &dr::dir_entry_view::unix_path)
      .def("__repr__", [](dr::dir_entry_view const& dev) {
        return fmt::format("dir_entry_view(inode={}, name={})",
                           dev.inode().inode_num(), dev.name());
      });

  py::class_<dr::directory_view>(mr, "directory_view")
      .def("inode", &dr::directory_view::inode)
      .def("parent_inode", &dr::directory_view::parent_inode)
      .def("size", &dr::directory_view::entry_count)
      .def(
          "__iter__",
          [](dr::directory_view& dv) {
            return py::make_iterator(dv.begin(), dv.end());
          },
          py::keep_alive<0, 1>())
      .def("__repr__", [](dr::directory_view const& dv) {
        return fmt::format("directory_view(inode={}, size={})", dv.inode(),
                           dv.entry_count());
      });

  py::enum_<dwarfs::posix_file_type::value>(m, "posix_file_type")
      .value("socket", dwarfs::posix_file_type::socket)
      .value("symlink", dwarfs::posix_file_type::symlink)
      .value("regular", dwarfs::posix_file_type::regular)
      .value("block", dwarfs::posix_file_type::block)
      .value("directory", dwarfs::posix_file_type::directory)
      .value("character", dwarfs::posix_file_type::character)
      .value("fifo", dwarfs::posix_file_type::fifo)
      .export_values();

  py::class_<dwarfs::file_stat>(m, "file_stat")
      .def(py::init<>())
      .def(py::init<fs::path const&>())
      .def("status", &dwarfs::file_stat::status)
      .def("type", &dwarfs::file_stat::type)
      .def_property("permissions", &dwarfs::file_stat::permissions,
                    &dwarfs::file_stat::set_permissions)
      .def_property("dev", &dwarfs::file_stat::dev, &dwarfs::file_stat::set_dev)
      .def_property("ino", &dwarfs::file_stat::ino, &dwarfs::file_stat::set_ino)
      .def_property("nlink", &dwarfs::file_stat::nlink,
                    &dwarfs::file_stat::set_nlink)
      .def_property("mode", &dwarfs::file_stat::mode,
                    &dwarfs::file_stat::set_mode)
      .def_property("uid", &dwarfs::file_stat::uid, &dwarfs::file_stat::set_uid)
      .def_property("gid", &dwarfs::file_stat::gid, &dwarfs::file_stat::set_gid)
      .def_property("rdev", &dwarfs::file_stat::rdev,
                    &dwarfs::file_stat::set_rdev)
      .def_property("size", &dwarfs::file_stat::size,
                    &dwarfs::file_stat::set_size)
      .def_property("blksize", &dwarfs::file_stat::blksize,
                    &dwarfs::file_stat::set_blksize)
      .def_property("blocks", &dwarfs::file_stat::blocks,
                    &dwarfs::file_stat::set_blocks)
      .def_property("atime", &dwarfs::file_stat::atime,
                    &dwarfs::file_stat::set_atime)
      .def_property("mtime", &dwarfs::file_stat::mtime,
                    &dwarfs::file_stat::set_mtime)
      .def_property("ctime", &dwarfs::file_stat::ctime,
                    &dwarfs::file_stat::set_ctime)
      .def("is_directory", &dwarfs::file_stat::is_directory)
      .def("is_regular_file", &dwarfs::file_stat::is_regular_file)
      .def("is_symlink", &dwarfs::file_stat::is_symlink)
      .def("is_device", &dwarfs::file_stat::is_device)
      .def("perm_string",
           py::overload_cast<>(&dwarfs::file_stat::perm_string, py::const_))
      .def("mode_string",
           py::overload_cast<>(&dwarfs::file_stat::mode_string, py::const_))
      .def("__repr__", [](dwarfs::file_stat const& fs) {
        return fmt::format("file_stat(mode={}, size={}, atime={}, mtime={}, "
                           "ctime={})",
                           fs.mode_string(), fs.size(), fs.atime(), fs.mtime(),
                           fs.ctime());
      });

  py::class_<dwarfs::performance_monitor,
             std::shared_ptr<dwarfs::performance_monitor>>(
      m, "performance_monitor")
      .def(py::init(&create_performance_monitor))
      .def("summary", [](dwarfs::performance_monitor const& pm) {
        std::ostringstream oss;
        pm.summarize(oss);
        return oss.str();
      });

  py::class_<std::future<dr::block_range>>(m, "block_range_future")
      .def("get", [](std::future<dr::block_range>& f) {
        auto br = f.get();
        return py::bytes(reinterpret_cast<char const*>(br.data()), br.size());
      });

  py::enum_<dr::readlink_mode>(mr, "readlink_mode")
      .value("raw", dr::readlink_mode::raw)
      .value("preferred", dr::readlink_mode::preferred)
      .value("posix", dr::readlink_mode::posix)
      .export_values();

  py::class_<dwarfs::vfs_stat>(m, "vfs_stat")
      .def(py::init<>())
      .def_readwrite("bsize", &dwarfs::vfs_stat::bsize)
      .def_readwrite("frsize", &dwarfs::vfs_stat::frsize)
      .def_readwrite("blocks", &dwarfs::vfs_stat::blocks)
      .def_readwrite("files", &dwarfs::vfs_stat::files)
      .def_readwrite("namemax", &dwarfs::vfs_stat::namemax)
      .def_readwrite("readonly", &dwarfs::vfs_stat::readonly)
      .def("__repr__", [](dwarfs::vfs_stat const& vs) {
        return fmt::format("vfs_stat(bsize={}, frsize={}, blocks={}, files={}, "
                           "namemax={}, readonly={})",
                           vs.bsize, vs.frsize, vs.blocks, vs.files, vs.namemax,
                           vs.readonly);
      });

  py::class_<dwarfs::history>(m, "history")
      .def(py::init<>())
      .def("data", [](dwarfs::history& h) { return h.as_json(); });

  py::class_<py_filesystem>(mr, "filesystem_v2")
      .def(py::init<dwarfs::logger&, dwarfs::os_access const&, fs::path const&,
                    std::shared_ptr<dwarfs::performance_monitor>>(),
           py::arg("logger"), py::arg("os_access"), py::arg("path"),
           py::arg("perfmon") = nullptr)
      .def("__enter__", &py_filesystem::enter)
      .def("__exit__", &py_filesystem::exit)
      .def("dump", &py_filesystem::dump)
      .def("find", &py_filesystem::find)
      .def("root", &py_filesystem::root)
      .def("open", &py_filesystem::open)
      .def("read", &py_filesystem::read, py::arg("inode"),
           py::arg("size") = std::numeric_limits<size_t>::max(),
           py::arg("offset") = 0)
      .def("readv", &py_filesystem::readv, py::arg("inode"),
           py::arg("size") = std::numeric_limits<size_t>::max(),
           py::arg("offset") = 0)
      .def("opendir", &py_filesystem::opendir)
      .def("readdir", &py_filesystem::readdir)
      .def("dirsize", &py_filesystem::dirsize)
      .def("walk", &py_filesystem::walk)
      .def("walk_data_order", &py_filesystem::walk_data_order)
      .def("info", &py_filesystem::info)
      .def("readlink", &py_filesystem::readlink)
      .def("getattr", &py_filesystem::getattr)
      .def("get_inode_info", &py_filesystem::get_inode_info)
      .def("get_metadata", &py_filesystem::get_metadata)
      .def("get_history", &py_filesystem::get_history)
      .def("statvfs", &py_filesystem::statvfs);
}
