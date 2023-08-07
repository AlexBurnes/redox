/*
* Redox - A modern, asynchronous, and wicked fast C++11 client for Redis
*
*    https://github.com/hmartiro/redox
*
* Copyright 2015 - Hayk Martirosyan <hayk.mart at gmail dot com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#pragma once

#include <hiredis/hiredis.h>
#include <hiredis/sds.h>

namespace redox {

    struct format_command {
        char *target;
        int len;
        const char *format;
        format_command(char *target, int len, const char *format);
        ~format_command();
        format_command(const format_command& r);
        format_command& operator=(const format_command& r) = delete;
        private:
            int **cnt;
    };

    inline const char* operator*(const format_command &s) {return s.format;};

    /**
     * The FormatCommand formats input format string with arguments to redis protocol formated buffer
     *
    */

    format_command FormatCommand(const char *format, va_list ap);
    format_command FormatCommand(const char *format, ...);


} // namespace redox
