/* BEGIN LICENSE
 * Copyright (C) 2011-2013 Percona Ireland Ltd.
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2, as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranties of
 * MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 * END LICENSE */

#include <cstdio>
#include <cstdlib>
#include <iostream>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <boost/foreach.hpp>
#include <boost/program_options.hpp>

#include <tbb/tbb_stddef.h>

#include <tbb/atomic.h>
#include <percona_playback/plugin.h>
#include <percona_playback/query_result.h>

class FullReportPlugin : public percona_playback::ReportPlugin
{
private:
  tbb::atomic<uint64_t> nr_select;
  tbb::atomic<uint64_t> nr_select_faster;
  tbb::atomic<uint64_t> nr_select_slower;
  tbb::atomic<uint64_t> nr_select_failed;
  tbb::atomic<uint64_t> nr_select_ignored;
  tbb::atomic<uint64_t> nr_update;
  tbb::atomic<uint64_t> nr_update_faster;
  tbb::atomic<uint64_t> nr_update_slower;
  tbb::atomic<uint64_t> nr_update_failed;
  tbb::atomic<uint64_t> nr_update_ignored;
  tbb::atomic<uint64_t> nr_insert;
  tbb::atomic<uint64_t> nr_insert_faster;
  tbb::atomic<uint64_t> nr_insert_slower;
  tbb::atomic<uint64_t> nr_insert_failed;
  tbb::atomic<uint64_t> nr_insert_ignored;
  tbb::atomic<uint64_t> nr_delete;
  tbb::atomic<uint64_t> nr_delete_faster;
  tbb::atomic<uint64_t> nr_delete_slower;
  tbb::atomic<uint64_t> nr_delete_failed;
  tbb::atomic<uint64_t> nr_delete_ignored;
  tbb::atomic<uint64_t> nr_replace;
  tbb::atomic<uint64_t> nr_replace_faster;
  tbb::atomic<uint64_t> nr_replace_slower;
  tbb::atomic<uint64_t> nr_replace_failed;
  tbb::atomic<uint64_t> nr_replace_ignored;
  tbb::atomic<uint64_t> nr_other;
  tbb::atomic<uint64_t> nr_other_faster;
  tbb::atomic<uint64_t> nr_other_slower;
  tbb::atomic<uint64_t> nr_other_failed;
  tbb::atomic<uint64_t> nr_other_ignored;
  tbb::atomic<uint64_t> total_execution_time_ms;
  tbb::atomic<uint64_t> expected_total_execution_time_ms;

  bool show_connection_query_count;

public:
  FullReportPlugin(std::string _name) : ReportPlugin(_name)
  {
    nr_select= 0;
    nr_select_faster= 0;
    nr_select_slower= 0;
    nr_select_failed= 0;
    nr_select_ignored= 0;
    nr_update= 0;
    nr_update_faster= 0;
    nr_update_slower= 0;
    nr_update_failed= 0;
    nr_update_ignored= 0;
    nr_insert= 0;
    nr_insert_faster= 0;
    nr_insert_slower= 0;
    nr_insert_failed= 0;
    nr_insert_ignored= 0;
    nr_delete= 0;
    nr_delete_faster= 0;
    nr_delete_slower= 0;
    nr_delete_failed= 0;
    nr_delete_ignored= 0;
    nr_replace= 0;
    nr_replace_faster= 0;
    nr_replace_slower= 0;
    nr_replace_failed= 0;
    nr_replace_ignored= 0;
    nr_other= 0;
    nr_other_faster= 0;
    nr_other_slower= 0;
    nr_other_failed= 0;
    nr_other_ignored= 0;
    total_execution_time_ms= 0;
    expected_total_execution_time_ms= 0;
  }


  virtual void query_execution(const uint64_t thread_id,
             const std::string &schema,
			       const std::string &query,
			       const QueryResult &expected,
			       const QueryResult &actual)
  {
    (void)thread_id;
    (void)expected;
    (void)actual;
    int faster= 0;
    int slower= 0;
    int failed= 0;
    int ignored= 0;

    total_execution_time_ms.fetch_and_add(actual.getDuration().total_microseconds());

    if (!actual.getError())
    {
      if (expected.getDuration().total_microseconds())
      {
        expected_total_execution_time_ms.fetch_and_add(expected.getDuration().total_microseconds());
        if (actual.getDuration().total_microseconds() <= expected.getDuration().total_microseconds())
        {
          faster= 1;
        }
        else
        {
          slower= 1;
        }
      }
    }
    else
    {
      if (!expected.getError())
      {
        failed= 1;
      }
      else
      {
        ignored= 1;
      }
    }

    std::string new_query = boost::to_upper_copy(query);

    if (new_query.find("SELECT ") != std::string::npos)
    {
		nr_select++;
		nr_select_faster+= faster;
    nr_select_slower+= slower;
    nr_select_failed+= failed;
    nr_select_ignored+= ignored;
    }
    else if (new_query.find("UPDATE ") != std::string::npos)
    {
	       	nr_update++;
		nr_update_faster+= faster;
    nr_update_slower+= slower;    
    nr_update_failed+= failed;
    nr_update_ignored+= ignored;
    }
    else if (new_query.find("INSERT ") != std::string::npos)
    {
	        nr_insert++;
		nr_insert_faster+= faster;
    nr_insert_slower+= slower;
    nr_insert_failed+= failed;
    nr_insert_ignored+= ignored;
    }
    else if (new_query.find("DELETE ") != std::string::npos)
    {
	        nr_delete++;
		nr_delete_faster+= faster;
    nr_delete_slower+= slower;
    nr_delete_failed+= failed;
    nr_delete_ignored+= ignored;
    }
    else if (new_query.find("REPLACE ") != std::string::npos)
    {
	        nr_replace++;
		nr_replace_faster+= faster;
    nr_replace_slower+= slower;
    nr_replace_failed+= failed;
    nr_replace_ignored+= ignored;
    }
    else
    {
 	        nr_other++;
		nr_other_faster+= faster;
    nr_other_slower+= slower;
    nr_other_failed+= failed;
    nr_other_ignored+= ignored;
    }

  }

  virtual void print_report()
  {
    printf(_("Detailed Report\n----------------\n"));
    printf(_("SELECTs  : %" PRIu64 " queries (%" PRIu64 " faster, %" PRIu64 " slower, %" PRIu64 " failed, %" PRIu64 " ignored)\n"), uint64_t(nr_select), uint64_t(nr_select_faster), uint64_t(nr_select_slower), uint64_t(nr_select_failed), uint64_t(nr_select_ignored));
    printf(_("INSERTs  : %" PRIu64 " queries (%" PRIu64 " faster, %" PRIu64 " slower, %" PRIu64 " failed, %" PRIu64 " ignored)\n"), uint64_t(nr_insert), uint64_t(nr_insert_faster), uint64_t(nr_insert_slower), uint64_t(nr_insert_failed), uint64_t(nr_insert_ignored));
    printf(_("UPDATEs  : %" PRIu64 " queries (%" PRIu64 " faster, %" PRIu64 " slower, %" PRIu64 " failed, %" PRIu64 " ignored)\n"), uint64_t(nr_update), uint64_t(nr_update_faster), uint64_t(nr_update_slower), uint64_t(nr_update_failed), uint64_t(nr_update_ignored));
    printf(_("DELETEs  : %" PRIu64 " queries (%" PRIu64 " faster, %" PRIu64 " slower, %" PRIu64 " failed, %" PRIu64 " ignored)\n"), uint64_t(nr_delete), uint64_t(nr_delete_faster), uint64_t(nr_delete_slower), uint64_t(nr_delete_failed), uint64_t(nr_delete_ignored));
    printf(_("REPLACEs : %" PRIu64 " queries (%" PRIu64 " faster, %" PRIu64 " slower, %" PRIu64 " failed, %" PRIu64 " ignored)\n"), uint64_t(nr_replace), uint64_t(nr_replace_faster), uint64_t(nr_replace_slower), uint64_t(nr_replace_failed), uint64_t(nr_replace_ignored));
    printf(_("<other>  : %" PRIu64 " queries (%" PRIu64 " faster, %" PRIu64 " slower, %" PRIu64 " failed, %" PRIu64 " ignored)\n\n\n"), uint64_t(nr_other), uint64_t(nr_other_faster), uint64_t(nr_other_slower), uint64_t(nr_other_failed), uint64_t(nr_other_ignored));
  }

};

static void init_plugin(percona_playback::PluginRegistry &r)
{
  r.add("full_report", new FullReportPlugin("full_report"));
}

PERCONA_PLAYBACK_PLUGIN(init_plugin);
