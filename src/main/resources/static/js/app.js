import {
  configureGenderHeader,
  getGenderFilters,
} from './header/genderHeader.js';
import {
  configurePeerInfluenceHeader,
  getPeerInfluenceFilters,
} from './header/peerInfluenceHeader.js';

import {
  configureAgeGroupHeader,
  getAgeGroupFilters,
} from './header/ageGroupHeader.js';

configureGenderHeader();
configurePeerInfluenceHeader();
configureAgeGroupHeader();

document.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    const ageGroupFilters = getAgeGroupFilters();
    console.log(ageGroupFilters);

    const genderFilters = getGenderFilters();
    const peerInfluenceFilters = getPeerInfluenceFilters();
    const requestUri = constructRequestUri(
      ageGroupFilters,
      genderFilters,
      peerInfluenceFilters
    );
    window.location.href = requestUri;
  }
});

function constructRequestUri(
  ageGroupFilters,
  genderFilters,
  peerInfluenceFilters
) {
  const uri = '/';
  const queryParameters = [];
  if (ageGroupFilters) {
    queryParameters.push(`ageGroup=${ageGroupFilters}`);
  }
  if (genderFilters) {
    queryParameters.push(`genders=${genderFilters}`);
  }
  if (peerInfluenceFilters) {
    queryParameters.push(`peerInfluence=${peerInfluenceFilters}`);
  }
  return `${uri}?${queryParameters.join('&')}`;
}
