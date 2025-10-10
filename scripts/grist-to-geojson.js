const https = require('https');
const fs = require('fs');

// Configuration depuis les variables d'environnement
const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_ID = 'Signalement';

if (!GRIST_DOC_ID || !GRIST_API_KEY) {
    console.error('❌ Variables d\'environnement manquantes');
    process.exit(1);
}

// Fonction pour récupérer les données Grist
function fetchGristData() {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'grist.dataregion.fr',
            path: `/o/docs/api/docs/${GRIST_DOC_ID}/tables/${TABLE_ID}/records`,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${GRIST_API_KEY}`,
                'Content-Type': 'application/json'
            }
        };

        https.get(options, (res) => {
            let data = '';

            res.on('data', (chunk) => {
                data += chunk;
            });

            res.on('end', () => {
                if (res.statusCode !== 200) {
                    reject(new Error(`Erreur API: ${res.statusCode} - ${data}`));
                } else {
                    resolve(JSON.parse(data));
                }
            });
        }).on('error', (err) => {
            reject(err);
        });
    });
}

// Conversion en GeoJSON
async function convertToGeoJSON() {
    try {
        console.log('🔄 Récupération des données depuis Grist...');
        const data = await fetchGristData();
        
        console.log(`✅ ${data.records.length} enregistrements récupérés`);
        
        // Construire le GeoJSON
        const features = data.records
            .filter(record => record.fields.geojson)
            .map(record => {
                try {
                    const geom = JSON.parse(record.fields.geojson);
                    
                    return {
                        type: 'Feature',
                        geometry: geom,
                        properties: {
                            id: record.id,
                            type: record.fields.type || 'Non spécifié',
                            description: record.fields.description || '',
                            date: record.fields.date || '',
                            geometrie_type: record.fields.geometrie_type || 'Point'
                        }
                    };
                } catch (e) {
                    console.warn(`⚠️ Erreur parsing geojson pour l'enregistrement ${record.id}:`, e.message);
                    return null;
                }
            })
            .filter(f => f !== null);
        
        const geojson = {
            type: 'FeatureCollection',
            features: features,
            metadata: {
                generated: new Date().toISOString(),
                source: 'Grist',
                count: features.length
            }
        };
        
        console.log(`✅ ${features.length} features créées`);
        
        // Écrire le fichier GeoJSON
        fs.writeFileSync('signalements.geojson', JSON.stringify(geojson, null, 2));
        console.log('✅ Fichier signalements.geojson créé avec succès !');
        
        // Créer un fichier de métadonnées
        const metadata = {
            lastUpdate: new Date().toISOString(),
            recordCount: features.length,
            pointCount: features.filter(f => f.geometry.type === 'Point').length,
            lineCount: features.filter(f => f.geometry.type === 'LineString').length,
            polygonCount: features.filter(f => f.geometry.type === 'Polygon').length
        };
        
        fs.writeFileSync('metadata.json', JSON.stringify(metadata, null, 2));
        console.log('✅ Métadonnées créées');
        
    } catch (error) {
        console.error('❌ Erreur:', error.message);
        process.exit(1);
    }
}

// Lancer la conversion
convertToGeoJSON();
