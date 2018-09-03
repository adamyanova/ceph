import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class ConfigurationService {
  constructor(private http: HttpClient) {}

  getConfigData() {
    return this.http.get('api/cluster_conf/');
  }
}
