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

#include "format.hpp"

namespace redox {

    format_command FormatCommand(const char *format, va_list ap) {
        format_command fs{nullptr, 0, format};
        fs.len = redisvFormatCommand(&fs.target,format,ap);
        return fs;
    }

    format_command FormatCommand(const char *format, ...) {
        format_command fs{nullptr, 0, format};
        va_list ap;
        va_start(ap,format);
        fs.len = redisvFormatCommand(&fs.target,format,ap);
        va_end(ap);
        return fs;
    }

    format_command::format_command(char *target, int len, const char *format)
        : target{nullptr}, len{0}, format{format}
        {
            cnt = new int*;
            *cnt = new int{0};
        }
        format_command::~format_command() {
            if (cnt) {
                if (**cnt) {
                    (**cnt)--;
                    return;
                }
                free(target);
                delete *cnt;
                delete cnt;
                cnt = nullptr;
            }
        }
        format_command::format_command(const format_command& r) {
            target = r.target;
            len = r.len;
            format = r.format;
            cnt = r.cnt;
            (**cnt)++;
        };

} //namespace redox
